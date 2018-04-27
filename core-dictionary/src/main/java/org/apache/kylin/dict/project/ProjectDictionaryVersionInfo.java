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

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
public class ProjectDictionaryVersionInfo extends RootPersistentEntity {
    @JsonProperty("key")
    private String key;

    @JsonProperty("max_version")
    private long maxVersion;

    @JsonProperty("id_length")
    private int idLength;

    public ProjectDictionaryVersionInfo() {
    }

    public ProjectDictionaryVersionInfo(String key, long version, int length) {
        this.key = pathToKey(key);
        this.maxVersion = version;
        this.idLength = length;
    }

    public String pathToKey(String baseDir) {
        return baseDir.replaceAll("\\/", ".");
    }

    @Override
    public String resourceName() {
        return key;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getMaxVersion() {
        return maxVersion;
    }

    public void setMaxVersion(long maxVersion) {
        this.maxVersion = maxVersion;
    }

    public int getIdLength() {
        return idLength;
    }

    public void setIdLength(int idLength) {
        this.idLength = idLength;
    }

    public ProjectDictionaryVersionInfo copy() {
        ProjectDictionaryVersionInfo copy = new ProjectDictionaryVersionInfo(this.getKey(), this.getMaxVersion(), this.getIdLength());
        copy.setLastModified(this.getLastModified());
        return copy ;
    }
}
