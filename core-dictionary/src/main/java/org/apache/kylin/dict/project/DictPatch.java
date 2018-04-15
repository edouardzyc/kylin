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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.kylin.common.persistence.RootPersistentEntity;


public class DictPatch extends RootPersistentEntity {
    private int[] offset;

    public DictPatch(int[] offset) {
        this.offset = offset;
    }

    public int[] getOffset() {
        return offset;
    }

    public void setOffset(int[] offset) {
        this.offset = offset;
    }

    public static class DictPatchSerializer implements org.apache.kylin.common.persistence.Serializer<DictPatch> {

        public static final DictPatchSerializer DICT_PATCH_SERIALIZER = new DictPatchSerializer();

        @Override
        public void serialize(DictPatch obj, DataOutputStream out) throws IOException {
            int length = obj.getOffset().length;
            out.writeInt(length);
            for (int i : obj.getOffset()) {
                out.writeInt(i);
            }
        }

        @Override
        public DictPatch deserialize(DataInputStream in) throws IOException {
            int length = in.readInt();
            int[] offsets = new int[length];
            for (int i = 0; i < length; i++) {
                offsets[i] = in.readInt();
            }

            return new DictPatch(offsets);
        }

    }

    public byte[] toByteArray() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DictPatchSerializer.DICT_PATCH_SERIALIZER.serialize(this, new DataOutputStream(out));
        return out.toByteArray();
    }

    public void writerTo(DataOutputStream out) throws IOException {
        DictPatchSerializer.DICT_PATCH_SERIALIZER.serialize(this, new DataOutputStream(out));
    }

    public DictPatch upgrade(DictPatch patch) {
        try {
            int[] originOffset = this.getOffset();
            int[] patchOffset = patch.getOffset();
            for (int i = 0; i < originOffset.length; i++) {
                originOffset[i] = patchOffset[originOffset[i]];
            }
        } catch (NullPointerException e) {
            System.out.println("s");
        }
        return this;
    }
}
