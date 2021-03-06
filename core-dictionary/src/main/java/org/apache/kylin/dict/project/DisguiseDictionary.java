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


@SuppressWarnings("serial")
public abstract class DisguiseDictionary {
    public static final int[] NULL_ID = new int[] {0, 0xff, 0xffff, 0xffffff, 0xffffffff};
    int idLength;

    /**
     * @param originId This segment id what the old dictionary encoding.
     * @return New id for project dictionary.
     */
    public abstract int upgrade(int originId);

    public int nullId() {
        return NULL_ID[idLength];
    }

    public boolean isNullId(int id) {
        int nullId = NULL_ID[idLength];
        return (nullId & id) == nullId;
    }

}
