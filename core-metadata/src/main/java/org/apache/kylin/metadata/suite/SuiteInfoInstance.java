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

package org.apache.kylin.metadata.suite;

import java.util.List;
import java.util.Objects;

import org.apache.kylin.common.persistence.RootPersistentEntity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class SuiteInfoInstance extends RootPersistentEntity {

    @JsonProperty("suite_id")
    private String suiteId;

    @JsonProperty("projects")
    private List<String> projects;

    public SuiteInfoInstance() {
    }

    public String getSuiteId() {
        return suiteId;
    }

    public void setSuiteId(String suiteId) {
        this.suiteId = suiteId;
    }

    public SuiteInfoInstance(String suiteId, List<String> projects) {
        this.suiteId = suiteId;

        this.projects = projects;
    }

    public List<String> getProjects() {
        return projects;
    }

    public void setProjects(List<String> projects) {
        this.projects = projects;
    }

    public void addProject(String project) {
        this.projects.add(project);
    }

    public void removeProject(String project) {
        this.projects.remove(project);
    }

    @Override
    public String resourceName() {
        return suiteId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(suiteId, projects);
    }

    @Override
    public String toString() {
        return "SuiteInfoInstance [suiteId=" + suiteId + ", projects=" + projects.toString() + "]";
    }
}
