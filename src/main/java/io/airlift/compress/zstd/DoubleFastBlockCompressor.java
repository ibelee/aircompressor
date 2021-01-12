/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.compress.zstd;

import static io.airlift.compress.zstd.Constants.SIZE_OF_INT;
import static io.airlift.compress.zstd.Constants.SIZE_OF_LONG;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;

class DoubleFastBlockCompressor
        implements BlockCompressor
{
    private static final int MIN_MATCH = 3; // 最小匹配长度
    private static final int SEARCH_STRENGTH = 8;   // 1<<8=256, 距离上个anchor 256bytes以内ip每次+1,每多256,多加1
    private static final int REP_MOVE = Constants.REPEATED_OFFSET_COUNT - 1;    // TODO:

    // caller: ZstdFrameCompressor.compressBlock
    // (inputBase, inputAddress, inputSize, context.sequenceStore, context.blockCompressionState, context.offsets, parameters);
    // inputAddress为一个Block的地址
    public int compressBlock(Object inputBase, final long inputAddress, int inputSize, SequenceStore output, BlockCompressionState state, RepeatedOffsets offsets, CompressionParameters parameters)
    {
        int matchSearchLength = Math.max(parameters.getSearchLength(), 4);
        // 匹配长度,至少为4, Level3,匹配长度为5

        // Offsets in hash tables are relative to baseAddress.
        // Hash tables can be reused across calls to compressBlock as long as baseAddress is kept constant.
        // We don't want to generate sequences that point before the current window limit,
        // so we "filter" out all results from looking up in the hash tables beyond that point.
        final long baseAddress = state.getBaseAddress();
        final long windowBaseAddress = baseAddress + state.getWindowBaseOffset();

        int[] longHashTable = state.hashTable;
        int longHashBits = parameters.getHashLog();

        int[] shortHashTable = state.chainTable;
        int shortHashBits = parameters.getChainLog();

        final long inputEnd = inputAddress + inputSize;
        // 指向一个Block的结尾
        final long inputLimit = inputEnd - SIZE_OF_LONG; // We read a long at a time for computing the hashes
        // 每次要读一段long计算 hash,所以设定一个limit

        long input = inputAddress;  // ip
        long anchor = inputAddress; // 存储每次match的结束位置,也就是下一组literals+match的开头地址

        int offset1 = offsets.getOffset0();
        int offset2 = offsets.getOffset1();
        // 把之前存的offset拿过来,可能是上一个block的,也可能是初始化为1,4,8的

        int savedOffset = 0;

        if (input - windowBaseAddress == 0) {
            input++;
        }// 让input至少指向windowBaseAddress的下一位置

        int maxRep = (int) (input - windowBaseAddress); // 允许的最大rep
        // 不允许超过window的范围去寻找匹配

        // offset超出的允许范围就归零,把之前的值先存下来
        if (offset2 > maxRep) {
            savedOffset = offset2;
            offset2 = 0;
        }

        if (offset1 > maxRep) {
            savedOffset = offset1;
            offset1 = 0;
        }

        while (input < inputLimit) {   // < instead of <=, because repcode check at (input+1)
            int shortHash = hash(inputBase, input, shortHashBits, matchSearchLength);
            long shortMatchAddress = baseAddress + shortHashTable[shortHash];

            int longHash = hash8(UNSAFE.getLong(inputBase, input), longHashBits);
            long longMatchAddress = baseAddress + longHashTable[longHash];

            // update hash tables
            int current = (int) (input - baseAddress);
            longHashTable[longHash] = current;
            shortHashTable[shortHash] = current;

            int matchLength;
            int offset;

            if (offset1 > 0 && UNSAFE.getInt(inputBase, input + 1 - offset1) == UNSAFE.getInt(inputBase, input + 1)) {
                // found a repeated sequence of at least 4 bytes, separated by offset1
                // 从input+1的位置,与input+1-offset1的位置有一个INT大小的匹配
                matchLength = count(inputBase, input + 1 + SIZE_OF_INT, inputEnd, input + 1 + SIZE_OF_INT - offset1) + SIZE_OF_INT;
                // 从这个INT匹配后继续找匹配,计算matchLength
                input++;
                // 找到匹配,input指向下一个位置,
                output.storeSequence(inputBase, anchor, (int) (input - anchor), 0, matchLength - MIN_MATCH);
                //  void storeSequence(Object literalBase, long literalAddress, int literalLength, int offsetCode, int matchLengthBase)
            }
            else {
                // check prefix long match
                // 检查longHash表中存放的从当前位置读一个Long的64位串计算得到的Hash值的上一次出现的位置,
                if (longMatchAddress > windowBaseAddress && UNSAFE.getLong(inputBase, longMatchAddress) == UNSAFE.getLong(inputBase, input)) {
                    matchLength = count(inputBase, input + SIZE_OF_LONG, inputEnd, longMatchAddress + SIZE_OF_LONG) + SIZE_OF_LONG;
                    offset = (int) (input - longMatchAddress);
                    while (input > anchor && longMatchAddress > windowBaseAddress && UNSAFE.getByte(inputBase, input - 1) == UNSAFE.getByte(inputBase, longMatchAddress - 1)) {
                        input--;
                        longMatchAddress--;
                        matchLength++;
                    }
                }
                else {
                    // check prefix short match
                    // 检查shortHash
                    if (shortMatchAddress > windowBaseAddress && UNSAFE.getInt(inputBase, shortMatchAddress) == UNSAFE.getInt(inputBase, input)) {
                        int nextOffsetHash = hash8(UNSAFE.getLong(inputBase, input + 1), longHashBits);
                        long nextOffsetMatchAddress = baseAddress + longHashTable[nextOffsetHash];
                        longHashTable[nextOffsetHash] = current + 1;

                        // check prefix long +1 match
                        // 如果找到了短匹配先不存,看看ip+1位置有没有长匹配
                        if (nextOffsetMatchAddress > windowBaseAddress && UNSAFE.getLong(inputBase, nextOffsetMatchAddress) == UNSAFE.getLong(inputBase, input + 1)) {
                            matchLength = count(inputBase, input + 1 + SIZE_OF_LONG, inputEnd, nextOffsetMatchAddress + SIZE_OF_LONG) + SIZE_OF_LONG;
                            input++;
                            offset = (int) (input - nextOffsetMatchAddress);
                            while (input > anchor && nextOffsetMatchAddress > windowBaseAddress && UNSAFE.getByte(inputBase, input - 1) == UNSAFE.getByte(inputBase, nextOffsetMatchAddress - 1)) {
                                input--;
                                nextOffsetMatchAddress--;
                                matchLength++;
                            }
                        }
                        else {
                            // if no long +1 match, explore the short match we found
                            // 如果ip+1位置没有长匹配,则使用ip位置的短匹配,前后扩充下
                            matchLength = count(inputBase, input + SIZE_OF_INT, inputEnd, shortMatchAddress + SIZE_OF_INT) + SIZE_OF_INT;
                            offset = (int) (input - shortMatchAddress);
                            while (input > anchor && shortMatchAddress > windowBaseAddress && UNSAFE.getByte(inputBase, input - 1) == UNSAFE.getByte(inputBase, shortMatchAddress - 1)) {
                                input--;
                                shortMatchAddress--;
                                matchLength++;
                            }
                        }
                    }
                    else {
                        input += ((input - anchor) >> SEARCH_STRENGTH) + 1;
                        continue;
                        // ip位置并没有找到任何匹配, ip往前走, 走多长与距离anchor中有多少个256bytes相关
                        // 由于空间的局部性,我们认为一个匹配的附近往往有更多的匹配,所以走的细一点
                    }
                }

                
                offset2 = offset1;
                offset1 = offset;
                // update offset 

                output.storeSequence(inputBase, anchor, (int) (input - anchor), offset + REP_MOVE, matchLength - MIN_MATCH);
                // void storeSequence(Object literalBase, long literalAddress, int literalLength, int offsetCode, int matchLengthBase)
            }

            input += matchLength;
            anchor = input;
            // 上一个literals+match结束, 相应指针移动到新一个literals的位置

            if (input <= inputLimit) {
                // 把ip前后2位置的信息填到hash表中,为后面的寻找匹配做贡献
                longHashTable[hash8(UNSAFE.getLong(inputBase, baseAddress + current + 2), longHashBits)] = current + 2;
                shortHashTable[hash(inputBase, baseAddress + current + 2, shortHashBits, matchSearchLength)] = current + 2;

                longHashTable[hash8(UNSAFE.getLong(inputBase, input - 2), longHashBits)] = (int) (input - 2 - baseAddress);
                shortHashTable[hash(inputBase, input - 2, shortHashBits, matchSearchLength)] = (int) (input - 2 - baseAddress);

                // 检查offset2
                // 此时ip指向上一个match的结尾,如果存在一个新的match,那么literals_length=0,
                // 反复检查offset1和2, 为什么是while而不是if呢,因为每次找到一个match会swap offset1和offset2, 所以循环检查
                while (input <= inputLimit && offset2 > 0 && UNSAFE.getInt(inputBase, input) == UNSAFE.getInt(inputBase, input - offset2)) {
                    int repetitionLength = count(inputBase, input + SIZE_OF_INT, inputEnd, input + SIZE_OF_INT - offset2) + SIZE_OF_INT;

                    // swap offset2 <=> offset1
                    int temp = offset2;
                    offset2 = offset1;
                    offset1 = temp;

                    shortHashTable[hash(inputBase, input, shortHashBits, matchSearchLength)] = (int) (input - baseAddress);
                    longHashTable[hash8(UNSAFE.getLong(inputBase, input), longHashBits)] = (int) (input - baseAddress);

                    output.storeSequence(inputBase, anchor, 0, 0, repetitionLength - MIN_MATCH);
                    // offset2找到的存 0,0,match_length-3

                    input += repetitionLength;
                    anchor = input;
                }
            }
        }

        // save reps for next block
        offsets.saveOffset0(offset1 != 0 ? offset1 : savedOffset);
        offsets.saveOffset1(offset2 != 0 ? offset2 : savedOffset);

        // return the last literals size
        return (int) (inputEnd - anchor);
    }

    // TODO: same as LZ4RawCompressor.count

    /**
     * matchAddress must be < inputAddress
     */
    // 统计inputAddress开始的内容与matchAddress开始的内容有多少个字节相匹配
    public static int count(Object inputBase, final long inputAddress, final long inputLimit, final long matchAddress)
    {
        long input = inputAddress;
        long match = matchAddress;

        int remaining = (int) (inputLimit - inputAddress);

        // first, compare long at a time
        int count = 0;
        // 以long为单位进行比较
        while (count < remaining - (SIZE_OF_LONG - 1)) {
            long diff = UNSAFE.getLong(inputBase, match) ^ UNSAFE.getLong(inputBase, input);
            // 取两个long进行异或,为0则相等
            if (diff != 0) {
                return count + (Long.numberOfTrailingZeros(diff) >> 3);
                // numberOfTrailingZeros: 数一个long数据的在出现第一个1之前有几个0,全0则返回64
                // 把这个结果整除8,即可得到有几个bytes匹配
            }

            count += SIZE_OF_LONG;
            input += SIZE_OF_LONG;
            match += SIZE_OF_LONG;
            // 匹配了4bytes,全部前移
        }

        // 当不够再读取一个long的时候到这里以字节为单位进行匹配
        while (count < remaining && UNSAFE.getByte(inputBase, match) == UNSAFE.getByte(inputBase, input)) {
            count++;
            input++;
            match++;
        }

        return count;
    }

    // matchSearchLength选择hash的类型,bits选择hash的位数
    private static int hash(Object inputBase, long inputAddress, int bits, int matchSearchLength)
    {
        switch (matchSearchLength) {
            case 8:
                return hash8(UNSAFE.getLong(inputBase, inputAddress), bits);
            case 7:
                return hash7(UNSAFE.getLong(inputBase, inputAddress), bits);
            case 6:
                return hash6(UNSAFE.getLong(inputBase, inputAddress), bits);
            case 5:
                return hash5(UNSAFE.getLong(inputBase, inputAddress), bits);
            default:
                return hash4(UNSAFE.getInt(inputBase, inputAddress), bits);
        }
    }

    private static final int PRIME_4_BYTES = 0x9E3779B1;
    private static final long PRIME_5_BYTES = 0xCF1BBCDCBBL;
    private static final long PRIME_6_BYTES = 0xCF1BBCDCBF9BL;
    private static final long PRIME_7_BYTES = 0xCF1BBCDCBFA563L;
    private static final long PRIME_8_BYTES = 0xCF1BBCDCB7A56463L;

    private static int hash4(int value, int bits)
    {
        return (value * PRIME_4_BYTES) >>> (Integer.SIZE - bits);
    }

    private static int hash5(long value, int bits)
    {
        return (int) (((value << (Long.SIZE - 40)) * PRIME_5_BYTES) >>> (Long.SIZE - bits));
    }

    private static int hash6(long value, int bits)
    {
        return (int) (((value << (Long.SIZE - 48)) * PRIME_6_BYTES) >>> (Long.SIZE - bits));
    }

    private static int hash7(long value, int bits)
    {
        return (int) (((value << (Long.SIZE - 56)) * PRIME_7_BYTES) >>> (Long.SIZE - bits));
    }

    private static int hash8(long value, int bits)
    {
        return (int) ((value * PRIME_8_BYTES) >>> (Long.SIZE - bits));
    }
}
