/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.dict.project;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.lemire.integercompression.BinaryPacking;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.SkippableComposition;
import me.lemire.integercompression.SkippableIntegerCODEC;
import me.lemire.integercompression.VariableByte;
import me.lemire.integercompression.differential.IntegratedBinaryPacking;
import me.lemire.integercompression.differential.IntegratedVariableByte;
import me.lemire.integercompression.differential.SkippableIntegratedComposition;

public class IntegratedUtils {
    private static final Logger log = LoggerFactory.getLogger(IntegratedUtils.class);
    private static final SkippableIntegratedComposition sortedCodec = new SkippableIntegratedComposition(
            new IntegratedBinaryPacking(), new IntegratedVariableByte());
    private final static SkippableIntegerCODEC unsortedCodec = new SkippableComposition(new BinaryPacking(),
            new VariableByte());

    public static int[] compress(int[] ints) {
        int[] compressed = new int[ints.length + 1024];
        compressed[0] = ints.length;
        IntWrapper inputOffset = new IntWrapper(0);
        IntWrapper outputOffset = new IntWrapper(1);
        sortedCodec.headlessCompress(ints, inputOffset, ints.length, compressed, outputOffset, new IntWrapper(0));
        log.info("compressed from " + ints.length * 4 / 1024 + "KB to " + outputOffset.intValue() * 4 / 1024 + "KB");
        // we can repack the data: (optional)
        return Arrays.copyOf(compressed, outputOffset.intValue());
    }

    public static int[] unCompress(int[] compressed) {
        int size = compressed[0];
        if (size == 0) {
            return new int[0];
        }
        // compressed integers
        int[] recovered = new int[size];
        IntWrapper recOffset = new IntWrapper(0);
        sortedCodec.headlessUncompress(compressed, new IntWrapper(1), compressed.length, recovered, recOffset, size,
                new IntWrapper(0));
        return recovered;
    }

}
