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

package org.apache.kylin.cube.model.validation.rule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.validation.ValidateContext;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class FunctionRuleTest extends LocalFileMetadataTestCase {
    private static KylinConfig config;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        config = KylinConfig.getInstanceFromEnv();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGoodDesc() throws IOException {
        FunctionRule rule = new FunctionRule();

        File f = new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + "/cube_desc/ssb.json");
        CubeDesc desc = JsonUtil.readValue(new FileInputStream(f), CubeDesc.class);
        desc.init(config);
        ValidateContext vContext = new ValidateContext();
        rule.validate(desc, vContext);
        vContext.print(System.out);
        assertTrue(vContext.getResults().length == 0);
    }

    /**
     * duplicated Measures will be removed when expanding outer measures to internal measures
     */
    @Test
    public void testValidateMeasureNamesDuplicated() throws IOException {
        File f = new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + "/cube_desc/ssb.json");
        CubeDesc desc = JsonUtil.readValue(new FileInputStream(f), CubeDesc.class);

        MeasureDesc measureDescDuplicated = desc.getOuterMeasures().get(1);
        List<MeasureDesc> newMeasures = Lists.newArrayList(desc.getOuterMeasures());
        newMeasures.add(measureDescDuplicated);
        desc.setOuterMeasures(newMeasures);

        desc.init(config);
        assertEquals(4, desc.getMeasures().size());
    }

    @Test
    public void testMultiCountMeasure() throws IOException {
        File f = new File(LocalFileMetadataTestCase.LOCALMETA_TEST_DATA + "/cube_desc/ssb.json");
        CubeDesc desc = JsonUtil.readValue(new FileInputStream(f), CubeDesc.class);

        MeasureDesc measureDescDuplicated = desc.getOuterMeasures().get(1);

        MeasureDesc newMeasure = new MeasureDesc();
        newMeasure.setName("count_2");
        newMeasure.setFunction(
                FunctionDesc.newInstance("COUNT", measureDescDuplicated.getFunction().getParameter(), "bigint"));
        List<MeasureDesc> newMeasures = Lists.newArrayList(desc.getOuterMeasures());
        newMeasures.add(newMeasure);
        desc.setOuterMeasures(newMeasures);

        String[] measuresRefs = desc.getHbaseMapping().getColumnFamily()[0].getColumns()[0].getMeasureRefs();
        String[] newMeasuresRefs = new String[measuresRefs.length + 1];
        System.arraycopy(measuresRefs, 0, newMeasuresRefs, 0, measuresRefs.length);
        newMeasuresRefs[measuresRefs.length] = "COUNT_2";
        desc.getHbaseMapping().getColumnFamily()[0].getColumns()[0].setMeasureRefs(newMeasuresRefs);
        desc.init(config);

        ValidateContext vContext = new ValidateContext();
        new FunctionRule().validate(desc, vContext);
        vContext.print(System.out);
        assertTrue(vContext.getResults().length == 0);
    }
}
