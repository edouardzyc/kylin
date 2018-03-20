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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;

import org.apache.kylin.common.util.Dictionary;

// dict for query
public class SaveMemDict extends Dictionary<String> implements DictFileResource {
    private int[] pos;
    private RandomAccessFile raf;

    public SaveMemDict() { // default constructor for Writable interface
    }

    public SaveMemDict(String path) throws IOException {
        raf = new RandomAccessFile(path, "r");
        int len = raf.readInt();

        pos = new int[len];
        for (int i = 0; i < pos.length; i++) {
            pos[i] = raf.readInt();
        }
    }

    public int getMinId() {
        return 0;
    }

    @Override
    public int getMaxId() {
        return pos.length - 1;
    }

    @Override
    public int getSizeOfId() {
        return 4; //size of int
    }

    @Override
    public int getSizeOfValue() {
        throw new UnsupportedOperationException("Dict only for query");
    }

    @Override
    public boolean contains(Dictionary<?> another) {
        throw new UnsupportedOperationException("Dict only for query");
    }

    @Override
    protected int getIdFromValueImpl(String value, int roundingFlag) {
        throw new UnsupportedOperationException("Dict only for query");
    }

    @Override
    protected String getValueFromIdImpl(int id) {
        throw new UnsupportedOperationException("Dict only for query");
    }

    @Override
    protected byte[] getValueBytesFromIdImpl(int id) {
        byte[] r;
        int base = 4 * pos.length + 4;
        int index;
        try {
            if (id == 0) {
                r = new byte[pos[0]];
                index = base;
            } else {
                int p = pos[id - 1];
                int l = pos[id] - p;
                r = new byte[l];
                index = p + base;
            }
            raf.seek(index);
            raf.read(r);
        } catch (IOException e) {
            return null;
        }
        return r;
    }

    @Override
    public void dump(PrintStream out) {
        throw new UnsupportedOperationException("Dict only for query");
    }

    @Override
    public void init() {}

    @Override
    public long getLastAccessTime() {
        return 0;
    }

    @Override
    public void setLastAccessTime(long mills) {

    }

    @Override
    public void write(DataOutput out) throws IOException {}

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSizeInBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void acquire() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void release() {
        try {
            raf.close();
        } catch (Exception e) {
            throw new RuntimeException("Can not release file mapping memory.", e);
        }
    }

    @Override
    public boolean isIdle() {
        return true;
    }

    @Override
    public void close() {
    }
}
