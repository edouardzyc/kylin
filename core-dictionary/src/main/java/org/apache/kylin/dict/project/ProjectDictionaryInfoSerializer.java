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

import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.JsonUtil;

public class ProjectDictionaryInfoSerializer implements Serializer<ProjectDictionaryInfo> {

    public static final ProjectDictionaryInfoSerializer FULL_SERIALIZER = new ProjectDictionaryInfoSerializer(false);
    public static final ProjectDictionaryInfoSerializer INFO_SERIALIZER = new ProjectDictionaryInfoSerializer(true);

    private boolean infoOnly;

    public ProjectDictionaryInfoSerializer() {
        this(false);
    }

    public ProjectDictionaryInfoSerializer(boolean infoOnly) {
        this.infoOnly = infoOnly;
    }

    @Override
    public void serialize(ProjectDictionaryInfo obj, DataOutputStream out) throws IOException {
        String json = JsonUtil.writeValueAsIndentString(obj);
        out.writeUTF(json);

        if (!infoOnly) {
            obj.getDictionaryObject().write(out);
        }
    }

    @Override
    public ProjectDictionaryInfo deserialize(DataInputStream in) throws IOException {
        String json = in.readUTF();
        ProjectDictionaryInfo obj = JsonUtil.readValue(json, ProjectDictionaryInfo.class);

        if (!infoOnly) {
            Dictionary<String> dict;
            try {
                dict = (Dictionary<String>) ClassUtil.forName(obj.getDictionaryClass(), Dictionary.class).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            dict.readFields(in);
            obj.setDictionaryObject(dict);
        }
        return obj;
    }

}