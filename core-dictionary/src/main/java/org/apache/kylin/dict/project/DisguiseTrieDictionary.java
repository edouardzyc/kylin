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

import com.google.common.base.Preconditions;

// todo check  override
@SuppressWarnings("serial")
public class DisguiseTrieDictionary<T> extends Dictionary<T> {
    private Dictionary<T> dictionary;
    private int[] offset;
    private int min;
    private int max;
    private int idLength;
    private boolean reverse = false;
    private int[] reverseOffset;

    public DisguiseTrieDictionary(int idLength, Dictionary<T> dictionary, DictPatch patch) {
        // for some dict no element
        if (patch != null && patch.getOffset().length == 0) {
            patch = null;
        }
        this.dictionary = dictionary;
        this.idLength = idLength;
        if (dictionary != null) {
            Preconditions.checkArgument(dictionary.getMinId() == 0);
            this.min = 0;
            this.max = dictionary.getMaxId();
        }
        if (patch != null) {
            this.offset = patch.getOffset();
            this.max = dictionary.getMaxId();

        }

    }
    @Override
    public int getMinId() {
        return this.min;
    }

    @Override
    public int getMaxId() {
        return this.max;
    }

    @Override
    public int getSizeOfId() {
        return idLength;
    }

    @Override
    public int getSizeOfValue() {
        return dictionary.getSizeOfValue();
    }

    @Override
    public boolean contains(Dictionary<?> another) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected T getValueFromIdImpl(int id) {
        if (isNull(id)) {
            return null;
        }
        return dictionary.getValueFromId(transformId(id));
    }

    public T getValueFromId(int id) throws IllegalArgumentException {
        if (isNull(id)) {
            return null;
        }
        return dictionary.getValueFromId(transformId(id));
    }

    @Override
    protected byte[] getValueBytesFromIdImpl(int id) {
        return dictionary.getValueByteFromId(transformId(id));
    }

    private int transformId(int id) {
        // todo remove offset.length = 0
        if (offset != null) {
            try {
                return offset[id - min];
            } catch (Exception e) {

                //  user to debug
                throw e;
            }
        } else {
            return id;
        }
    }

    private int transformReverseId(int id) {
        if (reverseOffset != null) {
            try {
                return reverseOffset[id];
            } catch (Exception e) {

                //  user to debug
                throw e;
            }
        } else {
            return id;
        }
    }

    @Override
    protected int getIdFromValueImpl(T value, int roundingFlag) {
        return dictionary.getIdFromValue(value, roundingFlag);
    }

    @Override
    public void dump(PrintStream out) {
        throw new UnsupportedOperationException();

    }

    public int getIdFromValue(T value, int roundingFlag) throws IllegalArgumentException {
        if (!reverse) {
            initReverse();
        }
        int id = transformReverseId(dictionary.getIdFromValue(value, roundingFlag));
        return id;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException();

    }

    private void initReverse() {
        if (offset != null) {
            reverseOffset = new int[getSize()];
            for (int i = 0; i < reverseOffset.length; i++) {
                // filter translation may provide hit missing values, need to return null on such case
                reverseOffset[i] =  NULL_ID[idLength];
            }
            for (int i = 0; i < offset.length; i++) {
                reverseOffset[offset[i]] = i;
            }
        }
        reverse = true;
    }

    private boolean isNull(int id) {
        int nullId = NULL_ID[idLength];
        return (nullId & id) == nullId;
    }
}
