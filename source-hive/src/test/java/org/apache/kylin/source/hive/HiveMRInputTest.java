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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HiveMRInputTest {

    @Before
    public void setup() {
        System.setProperty("log4j.configuration", "file:../build/conf/kylin-tools-log4j.properties");
        System.setProperty("KYLIN_CONF", LocalFileMetadataTestCase.LOCALMETA_TEST_DATA);
    }

    @Test
    public void testMaterializeViewHql() {
        final int viewSize = 2;
        String[] mockedViewNames = { "mockedView1", "mockedView2" };
        String[] mockedTalbeNames = { "mockedTable1", "mockedTable2" };
        String mockedWorkingDir = "mockedWorkingDir";

        StringBuilder hqls = new StringBuilder();
        for (int i = 0; i < viewSize; i++) {
            String hql = HiveMRInput.BatchCubingInputSide.materializeViewHql(mockedViewNames[i], mockedTalbeNames[i],
                    mockedWorkingDir);
            hqls.append(hql);
        }

        for (String sub : hqls.toString().split("\n")) {
            Assert.assertTrue(sub.endsWith(";"));
        }
        Assert.assertEquals("DROP TABLE IF EXISTS mockedView1;\n"
                + "CREATE TABLE IF NOT EXISTS mockedView1 LIKE mockedTable1 LOCATION 'mockedWorkingDir/mockedView1';\n"
                + "ALTER TABLE mockedView1 SET TBLPROPERTIES('auto.purge'='true');\n"
                + "INSERT OVERWRITE TABLE mockedView1 SELECT * FROM mockedTable1;\n"
                + "DROP TABLE IF EXISTS mockedView2;\n"
                + "CREATE TABLE IF NOT EXISTS mockedView2 LIKE mockedTable2 LOCATION 'mockedWorkingDir/mockedView2';\n"
                + "ALTER TABLE mockedView2 SET TBLPROPERTIES('auto.purge'='true');\n"
                + "INSERT OVERWRITE TABLE mockedView2 SELECT * FROM mockedTable2;\n", hqls.toString());
    }

    @Test
    public void testCleanupFlatHql() {
        HiveMRInput.GarbageCollectionStep garbageCollectionStep = new HiveMRInput.GarbageCollectionStep();
        garbageCollectionStep.setParam("oldHiveTable", "oldView");
        String hql1 = garbageCollectionStep.getCleanUpHql(KylinConfig.getInstanceFromEnv(), "oldTable1");
        Assert.assertEquals("hive -e \"USE default;\n" + "DROP TABLE IF EXISTS oldTable1;\n"
                + "\" --hiveconf fs.defaultFS=file:// --hiveconf mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec --hiveconf hive.merge.mapredfiles=true --hiveconf mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec --hiveconf dfs.replication=2 --hiveconf hive.exec.compress.output=true --hiveconf hive.auto.convert.join.noconditionaltask=true --hiveconf hive.merge.mapfiles=true --hiveconf hive.auto.convert.join.noconditionaltask.size=300000000 --hiveconf hive.merge.size.per.task=64000000",
                hql1);
        String hql2 = garbageCollectionStep.getCleanUpHql(KylinConfig.getInstanceFromEnv(), "oldTable1;oldTable2");
        Assert.assertEquals("hive -e \"USE default;\n" + "DROP TABLE IF EXISTS oldTable1;\n"
                + "DROP TABLE IF EXISTS oldTable2;\n"
                + "\" --hiveconf fs.defaultFS=file:// --hiveconf mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec --hiveconf hive.merge.mapredfiles=true --hiveconf mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec --hiveconf dfs.replication=2 --hiveconf hive.exec.compress.output=true --hiveconf hive.auto.convert.join.noconditionaltask=true --hiveconf hive.merge.mapfiles=true --hiveconf hive.auto.convert.join.noconditionaltask.size=300000000 --hiveconf hive.merge.size.per.task=64000000",
                hql2);
    }

}