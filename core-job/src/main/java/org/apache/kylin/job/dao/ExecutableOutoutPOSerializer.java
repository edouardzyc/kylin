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

package org.apache.kylin.job.dao;

import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.persistence.BrokenEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.JsonUtil;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class ExecutableOutoutPOSerializer implements Serializer<ExecutableOutputPO> {

    @Override
    public void serialize(ExecutableOutputPO obj, DataOutputStream out) throws IOException {
        JsonUtil.writeValueIndent(out, obj);
    }

    @Override
    public ExecutableOutputPO deserialize(DataInputStream in) throws IOException {
        final ByteBuffer buffer = toByteBuffer(in);
        buffer.mark();

        if (isBroken(buffer)) {
            final byte[] data = new byte[buffer.remaining()];
            buffer.get(data);
            final BrokenEntity brokenEntity = JsonUtil.readValue(data, BrokenEntity.class);

            final ExecutableOutputPO result = new ExecutableOutputPO();
            result.setUuid(getUuid(brokenEntity.getResPath()));
            result.setStatus("SUCCEED");
            result.setContent(brokenEntity.getErrorMsg());
            return result;
        }

        buffer.reset();
        return JsonUtil.readValue(buffer.array(), ExecutableOutputPO.class);
    }

    private ByteBuffer toByteBuffer(DataInputStream in) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        IOUtils.copy(in, out);

        final ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
        return buffer;
    }

    private boolean isBroken(final ByteBuffer buffer) {
        final byte[] magic = new byte[BrokenEntity.MAGIC.length];
        buffer.get(magic);
        return Arrays.equals(magic, BrokenEntity.MAGIC);
    }

    private String getUuid(final String resPath) {
        int cut = resPath.lastIndexOf("/");
        String uuid = resPath.substring(cut + 1);
        return uuid;
    }
}
