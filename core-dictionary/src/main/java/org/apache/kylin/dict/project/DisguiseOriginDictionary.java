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

public class DisguiseOriginDictionary extends DisguiseDictionary {

    DictPatch dictPatch;
    int sizeOfId;

    public DisguiseOriginDictionary(DictPatch maxVersionPatch, int sizeOfId) {
        super();
        this.dictPatch = maxVersionPatch;
        this.sizeOfId = sizeOfId;
    }

    @Override
    public int getMinId() {
        return 0;
    }

    @Override
    public int getMaxId() {
        return dictPatch.getOffset().length;
    }

    @Override
    public int getSizeOfId() {
        return sizeOfId;
    }

    @Override
    public int getSizeOfValue() {
        return sizeOfId;
    }

    @Override
    public boolean contains(Dictionary<?> another) {
        return false;
    }

    /**
     *
     * @param id eat something
     * @return  return = value
     * @throws IllegalArgumentException
     */
    @Override
    public String getValueFromId(int id) throws IllegalArgumentException {
        return id + "";
    }

    /**
     *
     * @param value eat something
     * @param roundingFlag  return = value
     * @return
     */
    @Override
    protected int getIdFromValueImpl(String value, int roundingFlag) {
        return Integer.parseInt(value);
    }

    @Override
    protected String getValueFromIdImpl(int id) {
        return id + "";
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
        return originId;
    }
}
