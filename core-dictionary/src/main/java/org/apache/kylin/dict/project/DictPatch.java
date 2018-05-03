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
    private int[] compressed;

    DictPatch(int[] offset) {
        this.offset = offset;
        this.compressed = IntegratedUtils.compress(offset);
    }

    private DictPatch(int[] offset, int[] compress) {
        this.offset = offset;
        this.compressed = compress;
    }

    public int[] getOffset() {
        return offset;
    }

    public static class DictPatchSerializer implements org.apache.kylin.common.persistence.Serializer<DictPatch> {

        public static final DictPatchSerializer DICT_PATCH_SERIALIZER = new DictPatchSerializer();

        @Override
        public void serialize(DictPatch obj, DataOutputStream out) throws IOException {
            int[] compressed = obj.compressed;
            int length = compressed.length;
            out.writeInt(length);
            for (int i : compressed) {
                out.writeInt(i);
            }
        }

        @Override
        public DictPatch deserialize(DataInputStream in) throws IOException {
            int length = in.readInt();
            int[] compress = new int[length];
            for (int i = 0; i < length; i++) {
                compress[i] = in.readInt();
            }
            int[] uncompress = IntegratedUtils.unCompress(compress);
            return new DictPatch(uncompress, compress);
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
        int[] newOffset = genNewOffset(patch.offset);
        return new DictPatch(newOffset);
    }

    protected int[] genNewOffset(int[] offset) {
        int[] newOffset = new int[this.getOffset().length];
        int[] originOffset = this.getOffset();
        for (int i = 0; i < originOffset.length; i++) {
            newOffset[i] = offset[originOffset[i]];
        }
        return newOffset;
    }
}
