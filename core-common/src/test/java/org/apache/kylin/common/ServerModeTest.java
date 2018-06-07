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

package org.apache.kylin.common;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ServerModeTest extends LocalFileMetadataTestCase {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }
    @Test
    public void happyTest() {
        // default is all
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Assert.assertTrue(ServerMode.isAll(config));
        Assert.assertTrue(ServerMode.isJob(config));
        Assert.assertTrue(ServerMode.isQuery(config));
        Assert.assertFalse(ServerMode.isMaster(config));

        // test job
        config.setProperty("kylin.server.mode", ServerMode.JOB.getName());
        Assert.assertTrue(ServerMode.isJob(config));
        Assert.assertFalse(ServerMode.isQuery(config));
        Assert.assertFalse(ServerMode.isAll(config));
        Assert.assertFalse(ServerMode.isMaster(config));

        // test query
        config.setProperty("kylin.server.mode", ServerMode.QUERY.getName());
        Assert.assertTrue(ServerMode.isQuery(config));
        Assert.assertFalse(ServerMode.isJob(config));
        Assert.assertFalse(ServerMode.isAll(config));
        Assert.assertFalse(ServerMode.isMaster(config));

        // test master
        config.setProperty("kylin.server.mode", ServerMode.MASTER.getName());
        Assert.assertTrue(ServerMode.isMaster(config));
        Assert.assertFalse(ServerMode.isQuery(config));
        Assert.assertFalse(ServerMode.isAll(config));
        Assert.assertFalse(ServerMode.isJob(config));

        // set config back
        config.setProperty("kylin.server.mode", ServerMode.ALL.getName());
    }

    @Test
    public void testBadCase() {
        expectedEx.expect(java.lang.AssertionError.class);
        KylinConfig config = null;
        ServerMode.isAll(config);
    }
}
