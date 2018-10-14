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

package org.apache.kylin.source.hive;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class FactDistinctColumnsMapperTest extends LocalFileMetadataTestCase {
    MapDriver mapDriver;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        FileUtils.deleteDirectory(new File("../source-hive/meta"));
        FileUtils.copyDirectory(new File(getTestConfig().getMetadataUrl().toString()), new File("../source-hive/meta"));
        FactDistinctColumnsMapper mapper = new FactDistinctColumnsMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        FileUtils.deleteDirectory(new File("../source-hive/meta"));
    }

    @Test
    public void testBasic() throws Exception {
        String cubeName = "test_kylin_cube_with_slr_1_new_segment";
        String segmentID = "198va32a-a33e-4b69-83dd-0bb8b1f8c53b";

        mapDriver.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
        mapDriver.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_ID, segmentID);
        mapDriver.getConfiguration().set(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT, "100");

        DefaultHCatRecord key = new DefaultHCatRecord(Lists.<Object> newArrayList("DEFAULT.TEST_KYLIN_FACT.CAL_DT",
                "EDW.TEST_CAL_DT.WEEK_BEG_DT", "DEFAULT.TEST_KYLIN_FACT.LEAF_CATEG_ID",
                "DEFAULT.TEST_KYLIN_FACT.LSTG_SITE_ID", "DEFAULT.TEST_CATEGORY_GROUPINGS.META_CATEG_NAME",
                "DEFAULT.TEST_CATEGORY_GROUPINGS.CATEG_LVL2_NAME", "DEFAULT.TEST_CATEGORY_GROUPINGS.CATEG_LVL3_NAME",
                "DEFAULT.TEST_KYLIN_FACT.LSTG_FORMAT_NAME", "DEFAULT.TEST_KYLIN_FACT.SLR_SEGMENT_CD",
                "DEFAULT.TEST_KYLIN_FACT.SELLER_ID", "DEFAULT.TEST_KYLIN_FACT.PRICE",
                "DEFAULT.TEST_KYLIN_FACT.ITEM_COUNT"));

        DefaultHCatRecord record1 = new DefaultHCatRecord(Lists.<Object> newArrayList("2012-01-01", 156614, 0,
                "Coins & Paper Money", "Paper Money: World", "Asia", "ABIN", 12, 10000294, 13, 23));

        DefaultHCatRecord record2 = new DefaultHCatRecord(Lists.<Object> newArrayList("2012-01-01", 156614, 0,
                "Coins & Paper Money", "Paper Money: World", "Asia", "ABIN", 13, 10000294, 14, 48));

        mapDriver.addInput(key, record1);
        mapDriver.addInput(key, record2);
        mapDriver.run();
        Counters counters = mapDriver.getCounters();
        long sumColumnBytes = 0L;
        for (Counter counter : counters.getGroup(BatchConstants.COLUMN_COUNTER_GROUP_NAME)) {
            sumColumnBytes =  sumColumnBytes + counter.getValue();
        }
        long rawDataCounter = counters.findCounter(FactDistinctColumnsMapper.RawDataCounter.BYTES).getValue();
        Assert.assertEquals(sumColumnBytes, rawDataCounter);
    }
}