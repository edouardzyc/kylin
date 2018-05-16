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
public class DisguiseUpgradeDictionary extends DisguiseDictionary {

    private DictPatch patch;
    private int[] offset;

    public DisguiseUpgradeDictionary(DictPatch patch, int idLength) {
        this.patch = patch;
        this.idLength = idLength;
        if (patch != null) {
            this.offset = patch.getOffset();
        }
    }

    public int nullId() {
        return NULL_ID[idLength];
    }

    @Override
    public int upgrade(int originId) {
        if (offset == null) {
            return originId;
        } else {
            if (isNullId(originId)) {
                return nullId();
            }
            return transformId(originId);
        }
    }

    public int transformId(int id) {
        if (offset != null) {
            try {
                return offset[id];
            } catch (Exception e) {
                // keep for debug
                throw e;
            }
        } else {
            return id;
        }
    }


}
