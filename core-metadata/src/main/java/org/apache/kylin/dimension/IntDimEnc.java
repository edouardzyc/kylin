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
 * deprecated use IntegerDimEnc instead
 * @deprecated
 */
public class IntDimEnc extends DimensionEncoding implements Serializable {
    private static final long serialVersionUID = 1L;

    private static Logger logger = LoggerFactory.getLogger(IntDimEnc.class);

    private static final long[] CAP = { 0, 0xffL, 0xffffL, 0xffffffL, 0xffffffffL, 0xffffffffffL, 0xffffffffffffL,
            0xffffffffffffffL, Long.MAX_VALUE };
    public static final String ENCODING_NAME = "int";

    public static class Factory extends DimensionEncodingFactory {
        @Override
        public String getSupportedEncodingName() {
            return ENCODING_NAME;
        }

        @Override
        public DimensionEncoding createDimensionEncoding(String encodingName, String[] args) {
            if (args.length > 1) {
                return new IntDimEnc(Integer.parseInt(args[0]), args[1]);
            }
            return new IntDimEnc(Integer.parseInt(args[0]));

        }
    }

    // ============================================================================

    private int fixedLen;

    transient private int avoidVerbose = 0;

    private ValueConvert valueConvert;
    private String dataType;

    //no-arg constructor is required for Externalizable
    public IntDimEnc() {

    }

    public IntDimEnc(int len) {
        if (len <= 0 || len >= CAP.length)
            throw new IllegalArgumentException("the length of IntDimEnc is " + len + ", which should be 1-8");

        this.fixedLen = len;
        this.valueConvert = new ValueConvert() {
            @Override
            public Object convert(long value) {
                return String.valueOf(value);
            }
        };
    }

    public IntDimEnc(int len, String dataType) {
        super();
        this.fixedLen = len;
        this.dataType = dataType;
        initCovert();
    }

    private void initCovert() {
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
        if (integer > CAP[fixedLen]) {
            if (avoidVerbose++ % 10000 == 0) {
                logger.warn("Expect at most " + fixedLen + " bytes, but got " + valueStr + ", will truncate, hit times:"
                        + avoidVerbose);
            }
        }

        BytesUtil.writeLong(integer, output, outputOffset, fixedLen);
    }

    @Override
    public Object decode(byte[] bytes, int offset, int len) {
        if (isNull(bytes, offset, len)) {
            return null;
        }
        return valueConvert.convert(doDecode(bytes, offset, len));
    }

    private long doDecode(byte[] bytes, int offset, int len) {
        return BytesUtil.readLong(bytes, offset, len);
    }

    @Override
    public DataTypeSerializer<Object> asDataTypeSerializer() {
        return new IntegerSerializer();
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

        IntDimEnc that = (IntDimEnc) o;

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
