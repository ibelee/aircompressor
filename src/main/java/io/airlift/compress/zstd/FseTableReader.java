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

import static io.airlift.compress.zstd.FiniteStateEntropy.MAX_SYMBOL;
import static io.airlift.compress.zstd.FiniteStateEntropy.MIN_TABLE_LOG;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static io.airlift.compress.zstd.Util.highestBit;
import static io.airlift.compress.zstd.Util.verify;

// FSE解码的时候从FSE流的开头读取数据获得tableLog,和normalized表,并构建解码表
class FseTableReader
{
    private final short[] nextSymbol = new short[MAX_SYMBOL + 1];
    private final short[] normalizedCounters = new short[MAX_SYMBOL + 1];

    public int readFseTable(FiniteStateEntropy.Table table, Object inputBase, long inputAddress, long inputLimit, int maxSymbol, int maxTableLog)
    {
        // read table headers
        long input = inputAddress;
        verify(inputLimit - inputAddress >= 4, input, "Not enough input bytes");

        int threshold;
        int symbolNumber = 0;
        boolean previousIsZero = false;

        int bitStream = UNSAFE.getInt(inputBase, input);    // 第一个byte, tableLog-5

        int tableLog = (bitStream & 0xF) + MIN_TABLE_LOG;   // 还原出tableLog

        int numberOfBits = tableLog + 1;    // 存储获得下一个数据需要读取的位数,初始化为tableLog+1,
        bitStream >>>= 4;
        int bitCount = 4;

        verify(tableLog <= maxTableLog, input, "FSE table size exceeds maximum allowed size");

        int remaining = (1 << tableLog) + 1;    // remaining=tableSize+1, 每次减proba, 最后为1
        threshold = 1 << tableLog;  // 判断用多少位的界限

        while (remaining > 1 && symbolNumber <= maxSymbol) {
            if (previousIsZero) {   // 0 后面的repeat flag
                int n0 = symbolNumber;
                while ((bitStream & 0xFFFF) == 0xFFFF) {
                    n0 += 24;
                    if (input < inputLimit - 5) {
                        input += 2;
                        bitStream = (UNSAFE.getInt(inputBase, input) >>> bitCount);
                    }
                    else {
                        // end of bit stream
                        bitStream >>>= 16;
                        bitCount += 16;
                    }
                }
                while ((bitStream & 3) == 3) {
                    n0 += 3;
                    bitStream >>>= 2;
                    bitCount += 2;
                }
                n0 += bitStream & 3;
                bitCount += 2;

                verify(n0 <= maxSymbol, input, "Symbol larger than max value");

                while (symbolNumber < n0) {
                    normalizedCounters[symbolNumber++] = 0;
                }
                if ((input <= inputLimit - 7) || (input + (bitCount >>> 3) <= inputLimit - 4)) {
                    input += bitCount >>> 3;
                    bitCount &= 7;
                    bitStream = UNSAFE.getInt(inputBase, input) >>> bitCount;
                }
                else {
                    bitStream >>>= 2;
                }
            }

            // For more details, see:
            // https://github.com/ibelee/zstd-1.4.8/blob/master/doc/zstd_compression_format.md#fse-table-description
            short max = (short) ((2 * threshold - 1) - remaining);  // max = 2*128 - 1 - 157 = 98
            short count;

            if ((bitStream & (threshold - 1)) < max) {
                count = (short) (bitStream & (threshold - 1));
                bitCount += numberOfBits - 1;
                // bitStream & 127, 低7bits
                // less than 98, count 就是读到的值value
                // 用7bits,所以回退1bit
            }
            else {
                count = (short) (bitStream & (2 * threshold - 1));
                // 得到8bits结果
                if (count >= threshold) {
                    count -= max;
                }
                // 最高位为1的,减去98进行对应
                // 最高位为0的,就是原value值
                bitCount += numberOfBits;
                // 8bits表示
            }
            count--;  // extra accuracy
            // proba = value - 1

            remaining -= Math.abs(count);   // remaining -= proba, -1情况特殊处理
            normalizedCounters[symbolNumber++] = count; // 写入normalizedCounters表
            previousIsZero = count == 0;    // 赋标志位
            while (remaining < threshold) { 
                // 调整threshold, 例如剩下的不到128了,那么肯定用不到8bits了,在7bits和6bits之间选择
                numberOfBits--;
                threshold >>>= 1;
            }

            if ((input <= inputLimit - 7) || (input + (bitCount >> 3) <= inputLimit - 4)) {
                input += bitCount >>> 3;
                bitCount &= 7;
                // input前进相应bytes,bitCount对8取模
            }
            else {
                bitCount -= (int) (8 * (inputLimit - 4 - input));
                input = inputLimit - 4;
            }
            bitStream = UNSAFE.getInt(inputBase, input) >>> (bitCount & 31);
        }

        verify(remaining == 1 && bitCount <= 32, input, "Input is corrupted");

        maxSymbol = symbolNumber - 1;
        verify(maxSymbol <= MAX_SYMBOL, input, "Max symbol value too large (too many symbols for FSE)");

        input += (bitCount + 7) >> 3;


        // 直接构建解码表
        // populate decoding table
        int symbolCount = maxSymbol + 1;
        int tableSize = 1 << tableLog;
        int highThreshold = tableSize - 1;

        table.log2Size = tableLog;

        // 先把-1的情况填到符号分布表的最后
        for (byte symbol = 0; symbol < symbolCount; symbol++) {
            if (normalizedCounters[symbol] == -1) {
                table.symbol[highThreshold--] = symbol;
                nextSymbol[symbol] = 1;
            }
            else {
                nextSymbol[symbol] = normalizedCounters[symbol];
            }
            // nextSymbol是一份normalized表的复制,做了-1到1的处理,用于构建解码表的++操作
        }

        // 构建符号分布表
        int position = FseCompressionTable.spreadSymbols(normalizedCounters, maxSymbol, tableSize, highThreshold, table.symbol);

        // position must reach all cells once, otherwise normalizedCounter is incorrect
        verify(position == 0, input, "Input is corrupted");

        // 从符号分布表的开头开始填解码表
        for (int i = 0; i < tableSize; i++) {
            byte symbol = table.symbol[i];
            short nextState = nextSymbol[symbol]++; // 每用一次该符号,++
            table.numberOfBits[i] = (byte) (tableLog - highestBit(nextState));
            table.newState[i] = (short) ((nextState << table.numberOfBits[i]) - tableSize);
        }

        return (int) (input - inputAddress);
    }

    public static void initializeRleTable(FiniteStateEntropy.Table table, byte value)
    {
        table.log2Size = 0;
        table.symbol[0] = value;
        table.newState[0] = 0;
        table.numberOfBits[0] = 0;
    }
}
