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

package org.apache.kylin.dimension;

import org.apache.kylin.common.util.ByteArray;
import org.junit.Assert;
import org.junit.Test;


public class FixedLenDimEncCompareTest {

    @Test
    public void testOrderAfterEncoding() {
        int length = 64;
        DimensionEncoding dimEnc = new FixedLenDimEnc(length);
        compare(dimEnc, "", "\u0000");
        compare(dimEnc, "", "\u0001");
        compare(dimEnc, "\u0000\u0001\u0001", "\u0000\u0001");
    }

    private void compare(DimensionEncoding dimEnc, String in1, String in2) {
        ByteArray out1 = new ByteArray(encode(dimEnc, in1));
        ByteArray out2 = new ByteArray(encode(dimEnc, in2));
        String decodeVal1 = encodeAndDecode(dimEnc, in1);
        String decodeVal2 = encodeAndDecode(dimEnc, in2);
        if (decodeVal1.compareTo(decodeVal2) > 0) {
            Assert.assertTrue(out1.compareTo(out2) > 0);
        } else if (decodeVal1.compareTo(decodeVal2) < 0) {
            Assert.assertTrue(out1.compareTo(out2) < 0);
        } else if (decodeVal1.compareTo(decodeVal2) == 0) {
            Assert.assertTrue(out1.compareTo(out2) == 0);
        }
    }

    private byte[] encode(DimensionEncoding dimEnc, String in) {
        byte[] output = new byte[dimEnc.getLengthOfEncoding()];
        dimEnc.encode(in, output, 0);
        return output;
    }

    private String encodeAndDecode(DimensionEncoding dimEnc, String in) {
        byte[] output = encode(dimEnc, in);
        return (String) dimEnc.decode(output, 0, dimEnc.getLengthOfEncoding());
    }
}
