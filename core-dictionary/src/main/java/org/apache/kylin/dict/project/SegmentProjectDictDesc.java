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

import com.fasterxml.jackson.annotation.JsonProperty;

public class SegmentProjectDictDesc {
    @JsonProperty("sourceIdentify")
    private String sourceIdentify;
    @JsonProperty("current_version")
    private long currentVersion;

    @JsonProperty("patch")
    private String patch;

    @JsonProperty("id_length")
    private int idLength;

    public SegmentProjectDictDesc() {
    }

    public SegmentProjectDictDesc(String sourceIdentify, long currentVersion, String patch, int idLength) {
        this.sourceIdentify = sourceIdentify;
        this.currentVersion = currentVersion;
        this.patch = patch;
        this.idLength = idLength;
    }

    public SegmentProjectDictDesc(String sourceIdentify, long currentVersion, int idLength) {
        this.sourceIdentify = sourceIdentify;
        this.currentVersion = currentVersion;
        this.idLength = idLength;
    }

    public int getIdLength() {
        return idLength;
    }

    public String getSourceIdentify() {
        return sourceIdentify;
    }

    public long getCurrentVersion() {
        return currentVersion;
    }

    public String getPatch() {
        return patch;
    }

    @Override
    public String toString() {
        return "SegmentProjectDictDesc{" +
                "sourceIdentify='" + sourceIdentify + '\'' +
                ", currentVersion=" + currentVersion +
                ", patch='" + patch + '\'' +
                ", idLength=" + idLength +
                '}';
    }
}
