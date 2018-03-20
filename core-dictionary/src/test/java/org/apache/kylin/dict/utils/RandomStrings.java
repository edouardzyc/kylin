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

package org.apache.kylin.dict.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

public class RandomStrings implements Iterable<String> {
    final private int size;

    public RandomStrings(int size) {
        this.size = size;
        //System.out.println("size = " + size);
    }

    @Override
    public Iterator<String> iterator() {
        return new Iterator<String>() {
            Random rand = new Random(System.currentTimeMillis());
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < size;
            }

            @Override
            public String next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }

                i++;
                //if (i % 1000000 == 0)
                //System.out.println(i);

                return nextString();
            }

            private String nextString() {
                StringBuffer buf = new StringBuffer();
                for (int i = 0; i < 64; i++) {
                    int v = rand.nextInt(16);
                    char c;
                    if (v >= 0 && v <= 9) {
                        c = (char) ('0' + v);
                    } else {
                        c = (char) ('a' + v - 10);
                    }
                    buf.append(c);
                }
                return buf.toString();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}