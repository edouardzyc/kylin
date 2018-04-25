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

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.source.IReadableTable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class ProjectDictionaryInfo extends DictionaryInfo {
    @JsonProperty("dictionary_version")
    private long dictionaryVersion;
    
    public ProjectDictionaryInfo() {
    }

    private ProjectDictionaryInfo(String sourceTable, String sourceColumn, int sourceColumnIndex, String dataType,
                                  IReadableTable.TableSignature input, Dictionary<String> dictionaryObject, String dictionaryClass,
                                  long dictionaryVersion) {
        super(sourceTable, sourceColumn, sourceColumnIndex, dataType, input);
        super.setDictionaryObject(dictionaryObject);
        super.setDictionaryClass(dictionaryClass);
        this.dictionaryVersion = dictionaryVersion;
    }



    public static ProjectDictionaryInfo wrap(DictionaryInfo dictionaryInfo, long version) {
        return new ProjectDictionaryInfo(dictionaryInfo.getSourceTable(), dictionaryInfo.getSourceColumn(),
                dictionaryInfo.getSourceColumnIndex(), dictionaryInfo.getDataType(), dictionaryInfo.getInput(),
                dictionaryInfo.getDictionaryObject(), dictionaryInfo.getDictionaryClass(), version);
    }

    public static ProjectDictionaryInfo copy(ProjectDictionaryInfo dictionaryInfo, Dictionary<String> dictionary) {
      return   new ProjectDictionaryInfo(dictionaryInfo.getSourceTable(), dictionaryInfo.getSourceColumn(),
                dictionaryInfo.getSourceColumnIndex(), dictionaryInfo.getDataType(), dictionaryInfo.getInput(),
                dictionary, dictionaryInfo.getDictionaryClass(), dictionaryInfo.getDictionaryVersion());
    }


    long getDictionaryVersion() {
        return dictionaryVersion;
    }

}
