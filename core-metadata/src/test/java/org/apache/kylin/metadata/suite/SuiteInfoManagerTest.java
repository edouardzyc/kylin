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

import com.google.common.collect.Lists;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

public class SuiteInfoManagerTest extends LocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws Exception {
        SuiteInfoManager mgr = SuiteInfoManager.getInstance(AbstractKylinTestCase.getTestConfig());

        {
            SuiteInfoInstance suiteInfo = mgr.getSuiteInfo("test-id");
            Assert.assertEquals(null, suiteInfo);
        }

        String suiteId = (UUID.randomUUID()).toString();
        {
            String[] projects = {"default"};
            SuiteInfoInstance suiteInfo = new SuiteInfoInstance(suiteId, Lists.newArrayList(projects));
            mgr.saveSuite(suiteInfo, false);
            suiteInfo = mgr.getSuiteInfo(suiteId);
            Assert.assertNotNull(suiteInfo);
            Assert.assertEquals(1, suiteInfo.getProjects().size());
        }

        {
            String[] projects = {"default", "default2"};
            SuiteInfoInstance suiteInfo = new SuiteInfoInstance(suiteId,  Lists.newArrayList(projects));
            mgr.saveSuite(suiteInfo, true);
            suiteInfo = mgr.getSuiteInfo(suiteId);
            Assert.assertNotNull(suiteInfo);
            Assert.assertEquals(2, suiteInfo.getProjects().size());
        }

        {
            mgr.addProjectToSuite(suiteId, "default3");
            SuiteInfoInstance suiteInfo = mgr.getSuiteInfo(suiteId);
            Assert.assertNotNull(suiteInfo);
            Assert.assertEquals(3, suiteInfo.getProjects().size());
        }

        {
            mgr.removeSuiteInfo(suiteId);
            Assert.assertEquals(0, mgr.listAllSuiteInfos().size());
        }
    }
}
