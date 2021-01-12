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

import static io.airlift.compress.zstd.Constants.SIZE_OF_LONG;  // 8
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

class SequenceStore
{
    public final byte[] literalsBuffer;
    public int literalsLength;  
    // literalsBuffer中已经存储的元素长度
    // 通过ARRAY_BYTE_BASE_OFFSET + literalsLength来找到写下一个元素的偏移
    public final int[] offsets;
    public final int[] literalLengths;
    public final int[] matchLengths;
    public int sequenceCount;   // the count of sequence

    public final byte[] literalLengthCodes;
    public final byte[] matchLengthCodes;
    public final byte[] offsetCodes;
    // length 和 Code 的关系
    // 对length进行分区,变成相应的Code加上读取的若干bits,FSE压缩的是Code, 把int转换成了byte

    public LongField longLengthField;   // TODO:
    public int longLengthPosition;

    public enum LongField
    {
        LITERAL, MATCH
    }

    // 两个length-code转换表
    private static final byte[] LITERAL_LENGTH_CODE = {0, 1, 2, 3, 4, 5, 6, 7,
                                                       8, 9, 10, 11, 12, 13, 14, 15,
                                                       16, 16, 17, 17, 18, 18, 19, 19,
                                                       20, 20, 20, 20, 21, 21, 21, 21,
                                                       22, 22, 22, 22, 22, 22, 22, 22,
                                                       23, 23, 23, 23, 23, 23, 23, 23,
                                                       24, 24, 24, 24, 24, 24, 24, 24,
                                                       24, 24, 24, 24, 24, 24, 24, 24};

    private static final byte[] MATCH_LENGTH_CODE = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                                                     16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                                                     32, 32, 33, 33, 34, 34, 35, 35, 36, 36, 36, 36, 37, 37, 37, 37,
                                                     38, 38, 38, 38, 38, 38, 38, 38, 39, 39, 39, 39, 39, 39, 39, 39,
                                                     40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
                                                     41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
                                                     42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                                                     42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42};
    // caller: CompressionContext.CompressionContext()
    //  构造函数,分配空间
    public SequenceStore(int blockSize, int maxSequences)
    {
        offsets = new int[maxSequences];
        literalLengths = new int[maxSequences];
        matchLengths = new int[maxSequences];
        // 3个int数组

        literalLengthCodes = new byte[maxSequences];
        matchLengthCodes = new byte[maxSequences];
        offsetCodes = new byte[maxSequences];
        // 3个byte数组

        literalsBuffer = new byte[blockSize];
        // literalsBuffer

        reset();
        // 该归零的归零
    }

    // caller: ZstdFrameCompressor.compressBlock()
    // copy literals from input buffer to literals buffer
    public void appendLiterals(Object inputBase, long inputAddress, int inputSize)
    {
        UNSAFE.copyMemory(inputBase, inputAddress, literalsBuffer, ARRAY_BYTE_BASE_OFFSET + literalsLength, inputSize);
        literalsLength += inputSize;
    }

    public void storeSequence(Object literalBase, long literalAddress, int literalLength, int offsetCode, int matchLengthBase)
    {
        long input = literalAddress;
        // input用来指示原文中待copy到literalsBuffer中的偏移
        long output = ARRAY_BYTE_BASE_OFFSET + literalsLength;
        // 因为literals存储在一个BYTE数组中,所以用ARRAY_BYTE_BASE_OFFSET+数组中已经存的literals的长度来确定要写入的偏移
        int copied = 0; // 计数作用
        do {
            UNSAFE.putLong(literalsBuffer, output, UNSAFE.getLong(literalBase, input));
            input += SIZE_OF_LONG;
            output += SIZE_OF_LONG;
            copied += SIZE_OF_LONG;
        }
        while (copied < literalLength);
        // 这里8个8个写,写多了也没事,因为下次写的时候是根据ARRAY_BYTE_BASE_OFFSET + literalsLength;来找索引的,所以无所谓

        literalsLength += literalLength;

        if (literalLength > 65535) {
            longLengthField = LongField.LITERAL;
            longLengthPosition = sequenceCount;
            // 如果这个literal+match匹配项中的literal长度大于int的存储范围,标记一下
            // TODO: 出现下一个长的把这个覆盖了怎么办
            // TODO: 一个传入的int型数据怎么会大于65535呢
        }
        literalLengths[sequenceCount] = literalLength;

        offsets[sequenceCount] = offsetCode + 1;
        // offsets存的,是offsetCode+1

        if (matchLengthBase > 65535) {
            longLengthField = LongField.MATCH;
            longLengthPosition = sequenceCount;
            // TODO: 和literals同样的问题
        }

        matchLengths[sequenceCount] = matchLengthBase;

        sequenceCount++;
        // 上面填写以sequenceCount作为索引的三个数组
    }

    public void reset()
    {
        literalsLength = 0;
        sequenceCount = 0;
        longLengthField = null;
    }

    public void generateCodes()
    {
        for (int i = 0; i < sequenceCount; ++i) {
            literalLengthCodes[i] = (byte) literalLengthToCode(literalLengths[i]);
            offsetCodes[i] = (byte) Util.highestBit(offsets[i]);    // offset的Code就是offset的log_2 下取整
            matchLengthCodes[i] = (byte) matchLengthToCode(matchLengths[i]);
        }

        if (longLengthField == LongField.LITERAL) {
            literalLengthCodes[longLengthPosition] = Constants.MAX_LITERALS_LENGTH_SYMBOL;  // 35
        }
        if (longLengthField == LongField.MATCH) {
            matchLengthCodes[longLengthPosition] = Constants.MAX_MATCH_LENGTH_SYMBOL;   // 52
        }
        // literalLength
        // [0,64]   读表
        // [64,65535]   log_2   Code[6+19,15+19]
        // 65535以上    35
    }

    // TODO: 这个Java的实现方式和C reference中实现的一样么,感觉格式上有点不一样
    private static int literalLengthToCode(int literalLength)
    {
        if (literalLength >= 64) {
            return Util.highestBit(literalLength) + 19; // log_2 下取整  再加 19
        }
        else {
            return LITERAL_LENGTH_CODE[literalLength];  // 读表
        }
    }

    /*
     * matchLengthBase = matchLength - MINMATCH
     * (that's how it's stored in SequenceStore)
     */
    private static int matchLengthToCode(int matchLengthBase)
    {
        if (matchLengthBase >= 128) {
            return Util.highestBit(matchLengthBase) + 36;   // 同literals
        }
        else {
            return MATCH_LENGTH_CODE[matchLengthBase];
        }
    }
}
