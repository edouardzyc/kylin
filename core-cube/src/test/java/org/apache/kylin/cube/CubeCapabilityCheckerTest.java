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

package org.apache.kylin.cube;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CubeCapabilityCheckerTest {
    private TableDesc tableDesc;
    private List<TblColRef> colRefs;
    private String oldConfValue;
    private String oldProValue;

    public static String LOCALMETA_TEST_DATA = "../examples/test_case_data/localmeta";

    @Before
    public void setup() {
        oldConfValue = System.getProperty(KylinConfig.KYLIN_CONF);
        oldProValue = System.getProperty("prop");
        System.setProperty("prop", "true");
        if (System.getProperty(KylinConfig.KYLIN_CONF) == null && System.getenv(KylinConfig.KYLIN_CONF) == null)
            System.setProperty(KylinConfig.KYLIN_CONF, LOCALMETA_TEST_DATA);

        List<String> columnNames = Lists.newArrayList("name", "id", "age", "gender");
        List<String> tblColTypes = Lists.newArrayList("string", "bigint", "integer", "varchar(100)");
        tableDesc = TableDesc.mockup("test");
        colRefs = mockTblCols("test", columnNames, tblColTypes);

    }

    @After
    public void cleanUp() {
        colRefs.clear();
        if (oldConfValue == null) {
            System.clearProperty(KylinConfig.KYLIN_CONF);
        } else {
            System.setProperty(KylinConfig.KYLIN_CONF, oldConfValue);
        }

        if (oldProValue == null) {
            System.clearProperty("prop");
        } else {
            System.setProperty("prop", oldProValue);
        }
    }

    @Test
    @Ignore
    public void testDemesionAsMeasures() {
        List<FunctionDesc> funcDescList = Lists.newArrayList();
        // count(*)
        funcDescList.add(FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT, ParameterDesc.newInstance("1"), "bigint"));
        CubeCapabilityChecker.tryDimensionAsMeasures(funcDescList, new CapabilityResult(), Sets.newHashSet(colRefs),
                true);
        assertEquals(1, funcDescList.size());
        CubeCapabilityChecker.tryDimensionAsMeasures(funcDescList, new CapabilityResult(), Sets.newHashSet(colRefs),
                false);
        assertEquals(0, funcDescList.size());
        funcDescList.clear();

        funcDescList.add(
                FunctionDesc.newInstance(FunctionDesc.FUNC_MAX, ParameterDesc.newInstance(colRefs.get(0)), "double"));
        CubeCapabilityChecker.tryDimensionAsMeasures(funcDescList, new CapabilityResult(), Sets.newHashSet(colRefs),
                true);
        assertEquals(0, funcDescList.size());

        funcDescList.add(FunctionDesc.newInstance(FunctionDesc.FUNC_PERCENTILE,
                ParameterDesc.newInstance(colRefs.get(0)), "double"));
        CubeCapabilityChecker.tryDimensionAsMeasures(funcDescList, new CapabilityResult(), Sets.newHashSet(colRefs),
                true);
        assertEquals(1, funcDescList.size());
        CubeCapabilityChecker.tryDimensionAsMeasures(funcDescList, new CapabilityResult(), Sets.newHashSet(colRefs),
                false);
        assertEquals(0, funcDescList.size());
        funcDescList.clear();

        funcDescList.add(
                FunctionDesc.newInstance(FunctionDesc.FUNC_SUM, ParameterDesc.newInstance(colRefs.get(0)), "double"));
        CubeCapabilityChecker.tryDimensionAsMeasures(funcDescList, new CapabilityResult(), Sets.newHashSet(colRefs),
                true);
        assertEquals(1, funcDescList.size());
        CubeCapabilityChecker.tryDimensionAsMeasures(funcDescList, new CapabilityResult(), Sets.newHashSet(colRefs),
                false);
        assertEquals(0, funcDescList.size());
        funcDescList.clear();
    }

    private List<TblColRef> mockTblCols(String tableName, List<String> tblColRefs, List<String> colTypes) {
        TableDesc t = TableDesc.mockup(tableName);
        List<TblColRef> list = new ArrayList<>();
        for (int i = 0; i < tblColRefs.size(); i++) {
            TblColRef c = TblColRef.mockup(t, i, tblColRefs.get(i), colTypes.get(i));
            list.add(c);
        }
        return list;
    }

}