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

package org.apache.kylin.measure.basic;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LongIngesterTest extends LocalFileMetadataTestCase {
    private DataModelDesc model;

    @Before
    public void setUp() {
        createTestMetadata();
        model = DataModelManager.getInstance(getTestConfig()).getDataModelDesc("ci_inner_join_model");
    }

    @Test
    public void testCount() throws Exception {
        MeasureDesc desc = new MeasureDesc();
        desc.setName("test");
        desc.setFunction(
                FunctionDesc.newInstance("count", ParameterDesc.newInstance(model.findColumn("TRANS_ID")), "bigint"));

        LongIngester ingester = new LongIngester();
        Assert.assertEquals(0L, ingester.valueOf(new String[] { null }, desc, null).longValue());
        Assert.assertEquals(1L, ingester.valueOf(new String[] { "12" }, desc, null).longValue());
        Assert.assertEquals(1L, ingester.valueOf(new String[] { "" }, desc, null).longValue());
    }

}