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

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class CubeDesc_TopNRequiresSum_Test extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata("src/test/resources/meta_CubeDesc_TopNRequiresSum_Test");
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testTopNRequiresSum() {
        CubeDescManager mgr = CubeDescManager.getInstance(getTestConfig());

        // case when SUM(PRICE) already presents, check there is only one SUM(PRICE) called "GMV_SUM"
        {
            CubeDesc cube = mgr.getCubeDesc("ci_inner_join_cube");
            int sumPriceCount = 0;
            TblColRef colPrice = cube.getModel().findColumn("PRICE");
            for (MeasureDesc m : cube.getMeasures()) {
                FunctionDesc f = m.getFunction();
                if (f.isSum() && colPrice.equals(f.getParameter().getColRef())) {
                    sumPriceCount++;
                    if (!"GMV_SUM".equals(m.getName()))
                        throw new IllegalStateException();
                }
            }
            if (sumPriceCount != 1)
                throw new IllegalStateException();
        }

        // case when SUM(PRICE) not presents, check there is SUM(PRICE) added by TopN
        {
            CubeDesc cube = mgr.getCubeDesc("topn_without_sum_cube");
            MeasureDesc m1 = cube.getMeasures().get(1);
            MeasureDesc m2 = cube.getMeasures().get(2);
            Assert.assertEquals("TOP_SELLER_0", m1.getName());
            Assert.assertEquals(
                    "FunctionDesc [expression=TOP_N, parameter=DEFAULT.TEST_KYLIN_FACT.PRICE,DEFAULT.TEST_KYLIN_FACT.SELLER_ID, returnType=topn(100, 4)]",
                    m1.getFunction().toString());
            Assert.assertEquals("TOP_SELLER_1", m2.getName());
            Assert.assertEquals(
                    "FunctionDesc [expression=SUM, parameter=DEFAULT.TEST_KYLIN_FACT.PRICE, returnType=decimal(19,4)]",
                    m2.getFunction().toString());
            
            HBaseColumnFamilyDesc cf1 = cube.getHbaseMapping().getColumnFamily()[1];
            Assert.assertEquals("F2", cf1.getName());
            HBaseColumnDesc hcol = cf1.getColumns()[0];
            Assert.assertEquals("HBaseColumnDesc [qualifier=M, measureRefs=[TOP_SELLER_0, TOP_SELLER_1]]", hcol.toString());
        }
    }
}