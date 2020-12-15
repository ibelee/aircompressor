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
// create the FSE compression table
import static io.airlift.compress.zstd.FiniteStateEntropy.MAX_SYMBOL;
// public static final int MAX_SYMBOL = 255;

class FseCompressionTable
{
    // FSE compression needs 2 tables.
    // CTable1: find the next state according to the old state.
    // CTable2: 2 items of every symbol
    private final short[] nextState;
    private final int[] deltaNumberOfBits;
    private final int[] deltaFindState;

    private int log2Size;
    //

    // caller: newInstance()
    // 给几个表分配足够大的数组空间
    public FseCompressionTable(int maxTableLog, int maxSymbol)
    {
        nextState = new short[1 << maxTableLog];
        deltaNumberOfBits = new int[maxSymbol + 1];
        deltaFindState = new int[maxSymbol + 1];
    }

    // My case: A:3, B:3, C:2
    // input: 
    // short[] normalizedCounts: the normalized frequency of each symbol
    // int maxSymbol: the maximum of the symbols
    // tablelog: tablelog is equal to 3 in my case
    public static FseCompressionTable newInstance(short[] normalizedCounts, int maxSymbol, int tableLog)
    {
        FseCompressionTable result = new FseCompressionTable(tableLog, maxSymbol);
        result.initialize(normalizedCounts, maxSymbol, tableLog);
        return result;
    }
    
    // caller: SequenceEncoder.compressSequences()
    public void initializeRleTable(int symbol)
    {
        log2Size = 0;

        nextState[0] = 0;
        nextState[1] = 0;

        deltaFindState[symbol] = 0;
        deltaNumberOfBits[symbol] = 0;
    }

    // caller: newInstance()
    // input: see newInstance()
    public void initialize(short[] normalizedCounts, int maxSymbol, int tableLog)
    {
        int tableSize = 1 << tableLog;
        // tableSize = 1 << 3 = 8 in my case

        byte[] table = new byte[tableSize]; // TODO: allocate in workspace
        // store the symbol which is spread
        int highThreshold = tableSize - 1;
        // highThreshold = 8 - 1 = 7 in my case

        // TODO: make sure FseCompressionTable has enough size
        log2Size = tableLog;

        // For explanations on how to distribute symbol values over the table:
        // http://fastcompression.blogspot.fr/2014/02/fse-distributing-symbol-values.html

        // symbol start positions
        int[] cumulative = new int[MAX_SYMBOL + 2]; // TODO: allocate in workspace
        // cumulative = [0,3,6,8,9] in my case
        // store the start position of every symbol
        cumulative[0] = 0;
        for (int i = 1; i <= maxSymbol + 1; i++) {
            if (normalizedCounts[i - 1] == -1) {  
                // Low probability symbol
                cumulative[i] = cumulative[i - 1] + 1;
                table[highThreshold--] = (byte) (i - 1);
            }
            else {
                cumulative[i] = cumulative[i - 1] + normalizedCounts[i - 1];
            }
        }
        cumulative[maxSymbol + 1] = tableSize + 1;
        // cumulative[3+1] = 8+1 = 9

        // Spread symbols
        int position = spreadSymbols(normalizedCounts, maxSymbol, tableSize, highThreshold, table);

        // make sure the symbols are successfully spread
        if (position != 0) {
            throw new AssertionError("Spread symbols failed");
        }

        // Build table
        // fill in CTable1
        for (int i = 0; i < tableSize; i++) {
            byte symbol = table[i];
            nextState[cumulative[symbol]++] = (short) (tableSize + i);  /* TableU16 : sorted by symbol order; gives next state value */
        }

        // Build symbol transformation table
        // fill in CTable2
        int total = 0;
        for (int symbol = 0; symbol <= maxSymbol; symbol++) {
            switch (normalizedCounts[symbol]) {
                case 0:
                    deltaNumberOfBits[symbol] = ((tableLog + 1) << 16) - tableSize;
                    break;
                case -1:
                case 1:
                    deltaNumberOfBits[symbol] = (tableLog << 16) - tableSize;
                    deltaFindState[symbol] = total - 1;
                    total++;
                    break;
                default:
                    int maxBitsOut = tableLog - Util.highestBit(normalizedCounts[symbol] - 1);
                    int minStatePlus = normalizedCounts[symbol] << maxBitsOut;
                    deltaNumberOfBits[symbol] = (maxBitsOut << 16) - minStatePlus;
                    deltaFindState[symbol] = total - normalizedCounts[symbol];
                    total += normalizedCounts[symbol];
                    break;
            }
        }
    }

    // 这个begin没理解好,如何找到FSE压缩的初始状态么
    public int begin(byte symbol)
    {
        int outputBits = (deltaNumberOfBits[symbol] + (1 << 15)) >>> 16;
        // 1000_0000_0000_0000 + deltaNumberOfBits[symbol], 
        int base = ((outputBits << 16) - deltaNumberOfBits[symbol]) >>> outputBits;
        return nextState[base + deltaFindState[symbol]];
    }

    // FSE compression
    public int encode(BitOutputStream stream, int state, int symbol)
    {
        int outputBits = (state + deltaNumberOfBits[symbol]) >>> 16;
        stream.addBits(state, outputBits);
        return nextState[(state >>> outputBits) + deltaFindState[symbol]];
    }

    public void finish(BitOutputStream stream, int state)
    {
        stream.addBits(state, log2Size);
        stream.flush();
    }

    // caller: spreadSymbols()
    // calculate the step, step = 5/8 * tableSize + 3
    private static int calculateStep(int tableSize)
    {
        return (tableSize >>> 1) + (tableSize >>> 3) + 3;
    }

    // caller: initialize()
    // 把符号按步长分配到symbols表中
    public static int spreadSymbols(short[] normalizedCounters, int maxSymbolValue, int tableSize, int highThreshold, byte[] symbols)
    {
        int mask = tableSize - 1;
        // mask = 8 - 1 = 7 in my case
        int step = calculateStep(tableSize);
        // step = 5/8 * tableSize + 3
        // in my case, the step is set to 3

        int position = 0;
        for (byte symbol = 0; symbol <= maxSymbolValue; symbol++) {
            for (int i = 0; i < normalizedCounters[symbol]; i++) {
                symbols[position] = symbol;
                do {
                    position = (position + step) & mask;
                }
                while (position > highThreshold);
            }
        }
        return position;
    }
}
