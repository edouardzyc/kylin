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

package org.apache.kylin.metadata.model;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.Before;
import org.junit.Test;

public class FunctionDescTest extends LocalFileMetadataTestCase {

    private DataModelDesc model;

    @Before
    public void setUp() {
        createTestMetadata();
        model = DataModelManager.getInstance(getTestConfig()).getDataModelDesc("ci_inner_join_model");
    }

    @Test
    public void testRewriteFieldName() {
        FunctionDesc function = FunctionDesc.newInstance("count",
                ParameterDesc.newInstance(model.findColumn("TRANS_ID")), "bigint");
        assertEquals("_KY_COUNT_TEST_KYLIN_FACT_TRANS_ID_", function.getRewriteFieldName());

        FunctionDesc function1 = FunctionDesc.newInstance("count", ParameterDesc.newInstance("1"), "bigint");
        assertEquals("_KY_COUNT__", function1.getRewriteFieldName());
    }

}