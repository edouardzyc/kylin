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

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * 这个测试用来测试MVC,稍后再补充
 */
@Ignore
public class MultiVersionControlTest extends LocalFileMetadataTestCase {
    String key = "default/DEFAULT.TEST_ACCOUNT/ACCOUNT_COUNTRY";
    MultiVersionControl multiVersionControl;
    Path baseDir;

    @Before
    public void init() throws IOException {
        staticCleanupTestMetadata();
        //        multiVersionControl = new MultiVersionControl(key);
        //        baseDir = multiVersionControl.getBaseDir();
    }

    @Test
    public void listAllVersions() throws IOException {

//        Long[] longs = multiVersionControl.listAllVersionWithHDFSs(FileSystem.get(new Configuration()), baseDir);
//        System.out.println(longs);
    }

    @Test
    public void getDictionaryVersion() {

    }

    @Test
    public void beginAppendWhenPreviousAppendCompleted() {

    }

    @Test
    public void beginAppend() {
    }

    @Test
    public void waitForPreviousAppendCompleted() {
    }

    @Test
    public void advanceMemstore() {
    }
}