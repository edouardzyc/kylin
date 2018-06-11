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

package org.apache.kylin.dict;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 */
public class Number2BytesConverterTest {

    @Test
    public void testBasics() {
        Number2BytesConverter conv = new Number2BytesConverter();
        assertEquals("0", conv.normalizeNumber("+0000.000"));
        assertEquals("0", conv.normalizeNumber("-0000.000"));
        assertEquals("0", conv.normalizeNumber("00.000"));
        assertEquals("123", conv.normalizeNumber("00123.000"));
        assertEquals("-123", conv.normalizeNumber("-0123"));
        assertEquals("-123.78", conv.normalizeNumber("-0123.780"));
        assertEquals("200", conv.normalizeNumber("200"));
        assertEquals("200", conv.normalizeNumber("200.00"));
        assertEquals("200.01", conv.normalizeNumber("200.010"));
        
        for (int i = -100; i < 101; i++) {
            String expected = "" + i;
            int cut = expected.startsWith("-") ? 1 : 0;
            String str = expected.substring(0, cut) + "00" + expected.substring(cut) + ".000";
            assertEquals(expected, conv.normalizeNumber(str));
        }
    }

}
