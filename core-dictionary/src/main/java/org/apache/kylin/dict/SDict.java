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

import org.apache.kylin.common.util.Dictionary;
import org.apache.spark.unsafe.Platform;
import sun.nio.ch.FileChannelImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.spark.unsafe.Platform.BYTE_ARRAY_OFFSET;

// dict for query
public class SDict extends Dictionary<String> implements DictFileResource {
    private int[] pos;
    private MappedByteBuffer byteBuffer;
    private SdictByteBuffer sdictByteBuffer;

    // only need when write dict
    private String[] values;

    // keep for closing
    private RandomAccessFile raf;
    private FileChannel fc;

    // one thread in use, occupations + 1
    private AtomicInteger occupations = new AtomicInteger(0);
    private AtomicLong accessTime = new AtomicLong(0L);


    public SDict() { // default constructor for Writable interface
    }

    public SDict(String path) throws FileNotFoundException {
        raf = new RandomAccessFile(path, "r");
    }

    public static SDict wrap(Dictionary dict) {
        String[] values = new String[dict.getSize()];
        for (int i = 0; i < values.length; i++) {
            values[i] = dict.getValueFromId(i).toString();
        }
        return new SDict(values);
    }

    public SDict(String[] values) {
        int total = 0;
        this.values = values;
        this.pos = new int[this.values.length];
        for (int i = 0; i < this.values.length; i++) {
            int currentLen = this.values[i].getBytes().length;
            this.pos[i] = (total += currentLen);
        }
    }

    @Override
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
        byte[] bytes = get(id);
        if (bytes == null) {
            return null;
        } else {
            return new String(bytes);
        }
    }

    @Override
    protected byte[] getValueBytesFromIdImpl(int id) {
        //  null id
        if (id > pos.length) {
            return null;
        }
        return get(id);
    }

    @Override
    public void dump(PrintStream out) {
        throw new UnsupportedOperationException("Dict only for query");
    }

    // lazy file mapping.
    @Override
    public void init() {
        try {
            fc = raf.getChannel();
            byteBuffer = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            sdictByteBuffer = new SdictByteBuffer(byteBuffer);
            int size = sdictByteBuffer.getInt();
            pos = new int[size];
            for (int i = 0; i < pos.length; i++) {
                pos[i] = sdictByteBuffer.getInt();
            }
        } catch (Exception e) {
            throw new RuntimeException("Can not init sdict.", e);
        }
    }

    @Override
    public long getLastAccessTime() {
        return accessTime.get();
    }

    @Override
    public void setLastAccessTime(long mills) {
        accessTime.set(mills);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // write head
        out.writeInt(pos.length);
        for (int length : pos) {
            out.writeInt(length);
        }

        // write body
        for (String value : values) {
            out.write(value.getBytes());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSizeInBytes() {
        try {
            return raf.length();
        } catch (IOException e) {
            throw new RuntimeException("Can not get file length.", e);
        }
    }

    @Override
    public void acquire() {
        occupations.incrementAndGet();
    }

    @Override
    public void release() {
        occupations.decrementAndGet();
    }

    @Override
    public boolean isIdle() {
        return occupations.get() == 0;
    }

    // thread safe
    private byte[] get(int id) {
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
            sdictByteBuffer.copyMemory(index, r, 0, r.length);
        } catch (ArrayIndexOutOfBoundsException e) {
            return null;
        }
        return r;
    }

    public int getLength(int id) {
        int length = 0;
        if (id == 0) {
            length = pos[0];
        } else {
            int p = pos[id - 1];
            length = pos[id] - p;
        }
        return length;
    }

    public void copyToByteArray(byte[] dst, int offset, int id, int length) {
        int base = 4 * pos.length + 4;
        int index;
        if (id == 0) {
            index = base;
        } else {
            int p = pos[id - 1];
            index = p + base;
        }
        sdictByteBuffer.copyMemory(index, dst, offset, length);
    }

    @Override
    public void close() {
        try {
            fc.close();
            raf.close();
            Method m = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
            m.setAccessible(true);
            m.invoke(FileChannelImpl.class, byteBuffer);
        } catch (Exception e) {
            throw new RuntimeException("Can not release file mapping memory.", e);
        }
    }


    public class SdictByteBuffer {
        ByteBuffer a;
        long baseAddress;


        public SdictByteBuffer(ByteBuffer buffer) {
            try {
                Field address = Buffer.class.getDeclaredField("address");
                address.setAccessible(true);
                this.baseAddress = (long) address.get(buffer);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
            this.a = buffer;
        }


        public int getInt() {
            return a.getInt();
        }


        public void copyMemory(int pos, byte[] dst, int offset, int length) {

            Platform.copyMemory(null, ix(pos), dst,
                    BYTE_ARRAY_OFFSET + ((long) offset << 0),
                    (long) length << 0);
        }

        private long ix(int i) {
            return baseAddress + ((long) i << 0);
        }
    }


}
