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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.kylin.common.util.Dictionary;

public class DisguiseUpgradeDictionary extends DisguiseDictionary {

    private DictPatch patch;
    private int idLength;
    private int[] offset;

    public DisguiseUpgradeDictionary(DictPatch patch, int idLength) {
        this.patch = patch;
        this.idLength = idLength;
        if (patch != null) {
            this.offset = patch.getOffset();
        }
    }

    @Override
    public int getMinId() {
        return 0;
    }

    @Override
    public int getMaxId() {
        return patch.getOffset().length;
    }

    @Override
    public int getSizeOfId() {
        return idLength;
    }

    @Override
    public int getSizeOfValue() {
        return 0;
    }

    @Override
    public boolean contains(Dictionary<?> another) {
        return false;
    }

    @Override
    protected int getIdFromValueImpl(String value, int roundingFlag) {
        if (patch == null) {
            return Integer.parseInt(value);
        }
        try {
            return patch.getOffset()[Integer.parseInt(value)];
        } catch (ArrayIndexOutOfBoundsException e) {
            return nullId();
        }
    }

    @Override
    protected String getValueFromIdImpl(int id) {
        return null;
    }

    @Override
    public void dump(PrintStream out) {

    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

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

                //  user to debug
                throw e;
            }
        } else {
            return id;
        }
    }
    public boolean isNull(int id) {
        int nullId = NULL_ID[idLength];
        return (nullId & id) == nullId;
    }
}
