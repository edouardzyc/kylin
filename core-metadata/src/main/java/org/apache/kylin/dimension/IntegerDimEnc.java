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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * replacement for IntDimEnc, the diff is IntegerDimEnc supports negative values
 * for IntegerDimEnc(N), the supported range is (-2^(8*N-1),2^(8*N-1))
 *
 * -2^(8*N-1) is not supported because the slot is reserved for null values.
 * -2^(8*N-1) will be encoded with warn, and its output will be null
 */
public class IntegerDimEnc extends DimensionEncoding implements Serializable {
    private static final long serialVersionUID = 1L;

    private static Logger logger = LoggerFactory.getLogger(IntegerDimEnc.class);

    private static final long[] CAP = { 0, 0x7fL, 0x7fffL, 0x7fffffL, 0x7fffffffL, 0x7fffffffffL, 0x7fffffffffffL,
            0x7fffffffffffffL, 0x7fffffffffffffffL };
    private static final long[] MASK = { 0, 0xffL, 0xffffL, 0xffffffL, 0xffffffffL, 0xffffffffffL, 0xffffffffffffL,
            0xffffffffffffffL, 0xffffffffffffffffL };
    private static final long[] TAIL = { 0, 0x80L, 0x8000L, 0x800000L, 0x80000000L, 0x8000000000L, 0x800000000000L,
            0x80000000000000L, 0x8000000000000000L };
    static {
        for (int i = 1; i < TAIL.length; ++i) {
            long head = ~MASK[i];
            TAIL[i] = head | TAIL[i];
        }
    }

    public static final String ENCODING_NAME = "integer";

    public static class Factory extends DimensionEncodingFactory {
        @Override
        public String getSupportedEncodingName() {
            return ENCODING_NAME;
        }

        @Override
        public DimensionEncoding createDimensionEncoding(String encodingName, String[] args) {
            if (args.length > 1) {
                return new IntegerDimEnc(Integer.parseInt(args[0]), args[1]);
            }
            return new IntegerDimEnc(Integer.parseInt(args[0]));

        }
    }

    // ============================================================================

    private int fixedLen;

    transient private int avoidVerbose = 0;
    transient private int avoidVerbose2 = 0;

    private ValueConvert valueConvert = new ValueConvert() {
        @Override
        public Object convert(long value) {
            return String.valueOf(value);
        }
    };;
    private String dataType;

    //no-arg constructor is required for Externalizable
    public IntegerDimEnc() {
    }

    public IntegerDimEnc(int len) {
        if (len <= 0 || len >= CAP.length)
            throw new IllegalArgumentException();

        this.fixedLen = len;
        this.dataType = "varchar";
        this.valueConvert = new ValueConvert() {
            @Override
            public Object convert(long value) {
                return String.valueOf(value);
            }
        };
    }

    public IntegerDimEnc(int len, String dataType) {
        if (len <= 0 || len >= CAP.length)
            throw new IllegalArgumentException();
        this.dataType = dataType;
        this.fixedLen = len;
        initCovert();
    }

    private void initCovert() {
        if(this.dataType == null){
            this.valueConvert = new ValueConvert() {
                @Override
                public Object convert(long value) {
                    return String.valueOf(value);
                }
            };
        }else {
            switch (this.dataType) {
                case "integer":
                    this.valueConvert = new ValueConvert() {
                        @Override
                        public Object convert(long value) {
                            return (int) value;
                        }
                    };
                    break;
                case "bigint":
                    this.valueConvert = new ValueConvert() {
                        @Override
                        public Object convert(long value) {
                            return value;
                        }
                    };
                    break;
                case "smallint":
                    this.valueConvert = new ValueConvert() {
                        @Override
                        public Object convert(long value) {
                            return (short) value;
                        }
                    };
                    break;
                case "tinyint":
                    this.valueConvert = new ValueConvert() {
                        @Override
                        public Object convert(long value) {
                            return (byte) value;
                        }
                    };
                    break;
                default:
                    this.valueConvert = new ValueConvert() {
                        @Override
                        public Object convert(long value) {
                            return String.valueOf(value);
                        }
                    };
                    break;
            }
        }
    }

    @Override
    public int getLengthOfEncoding() {
        return fixedLen;
    }

    @Override
    public void encode(String valueStr, byte[] output, int outputOffset) {
        if (valueStr == null) {
            Arrays.fill(output, outputOffset, outputOffset + fixedLen, NULL);
            return;
        }

        long integer = Long.parseLong(valueStr);
        if (integer > CAP[fixedLen] || integer < TAIL[fixedLen]) {
            if (avoidVerbose++ % 10000 == 0) {
                logger.warn("Expect at most " + fixedLen + " bytes, but got " + valueStr + ", will truncate, hit times:"
                        + avoidVerbose);
            }
        }

        if (integer == TAIL[fixedLen]) {
            if (avoidVerbose2++ % 10000 == 0) {
                logger.warn("Value " + valueStr + " does not fit into " + fixedLen + " bytes ");
            }
        }

        BytesUtil.writeLong(integer + CAP[fixedLen], output, outputOffset, fixedLen);//apply an offset to preserve binary order, overflow is okay
    }

    @Override
    public Object decode(byte[] bytes, int offset, int len) {
        if (isNull(bytes, offset, len)) {
            return null;
        }
        return valueConvert.convert(doDecode(bytes, offset, len));
    }

    private long doDecode(byte[] bytes, int offset, int len) {
        long integer = BytesUtil.readLong(bytes, offset, len) - CAP[fixedLen];

        //only take useful bytes
        integer = integer & MASK[fixedLen];
        boolean positive = (integer & ((0x80L) << ((fixedLen - 1) << 3))) == 0;
        if (!positive) {
            integer |= (~MASK[fixedLen]);
        }
        return integer;
    }

    @Override
    public DataTypeSerializer<Object> asDataTypeSerializer() {
        return new IntegerSerializer();
    }

    @Override
    public String returnType() {
        return dataType;
    }

    public class IntegerSerializer extends DataTypeSerializer<Object> {

        private byte[] currentBuf() {
            byte[] buf = (byte[]) current.get();
            if (buf == null) {
                buf = new byte[fixedLen];
                current.set(buf);
            }
            return buf;
        }

        @Override
        public void serialize(Object value, ByteBuffer out) {
            byte[] buf = currentBuf();
            String valueStr = value == null ? null : value.toString();
            encode(valueStr, buf, 0);
            out.put(buf);
        }

        @Override
        public Object deserialize(ByteBuffer in) {
            byte[] buf = currentBuf();
            in.get(buf);
            return decode(buf, 0, buf.length);
        }

        @Override
        public int peekLength(ByteBuffer in) {
            return fixedLen;
        }

        @Override
        public int maxLength() {
            return fixedLen;
        }

        @Override
        public int getStorageBytesEstimate() {
            return fixedLen;
        }

        @Override
        public Object valueOf(String str) {
            return str;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(fixedLen);
        out.writeUTF(dataType);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fixedLen = in.readShort();
        dataType = in.readUTF();
        initCovert();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        IntegerDimEnc that = (IntegerDimEnc) o;

        return fixedLen == that.fixedLen;

    }

    @Override
    public int hashCode() {
        return fixedLen;
    }

    interface ValueConvert extends Serializable {
        Object convert(long value);
    }
}
