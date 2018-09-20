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

package org.apache.kylin.rest.manager;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.dict.MockupReadableTable;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.dict.lookup.SnapshotTableV2;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class SnapshotManagerTest extends LocalFileMetadataTestCase {

    private KylinConfig kylinConfig;
    private SnapshotManager snapshotManager;
    List<String[]> expect;
    List<String[]> dif;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        String[] s1 = new String[] { "1", "CN" };
        String[] s2 = new String[] { "2", "NA" };
        String[] s3 = new String[] { "3", "NA" };
        String[] s4 = new String[] { "4", "KR" };
        String[] s5 = new String[] { "5", "JP" };
        String[] s6 = new String[] { "6", "CA" };
        expect = Lists.newArrayList(s1, s2, s3, s4, s5);
        dif = Lists.newArrayList(s1, s2, s3, s4, s6);

    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    private TableDesc genTableDesc(String tableName) {
        TableDesc table = TableDesc.mockup(tableName);
        ColumnDesc desc1 = new ColumnDesc("1", "id", "string", null, null, null, null);
        desc1.setId("1");
        ColumnDesc desc2 = new ColumnDesc("2", "country", "string", null, null, null, null);
        desc2.setId("2");
        ColumnDesc[] columns = { desc1, desc2 };
        table.setColumns(columns);
        return table;
    }

    private IReadableTable genTable(String path, List<String[]> content) {
        IReadableTable.TableSignature signature = new IReadableTable.TableSignature(path, content.size(), 0);
        return new MockupReadableTable(content, signature, true);
    }

    @Test
    public void testCheckByContent() throws IOException {
        runTestCase("SnapshotTableV1");
        runTestCase("SnapshotTableV2");
    }

    public void runTestCase(String className) throws IOException {
        System.setProperty("kylin.snapshot.impl-class", "org.apache.kylin.dict.lookup." + className);
        kylinConfig = KylinConfig.getInstanceFromEnv();
        snapshotManager = SnapshotManager.getInstance(kylinConfig);
        SnapshotTable origin = snapshotManager.buildSnapshot(genTable("./origin", expect), genTableDesc("TEST_TABLE"),
                kylinConfig);
        SnapshotTable dup = snapshotManager.buildSnapshot(genTable("./dup", expect), genTableDesc("TEST_TABLE"),
                kylinConfig);

        Assert.assertEquals(origin.getUuid(), dup.getUuid());
        SnapshotTable actual = snapshotManager.getSnapshotTable(origin.getResourcePath());
        IReadableTable.TableReader reader = actual.getReader();
        Assert.assertEquals(expect.size(), actual.getRowCount());
        int i = 0;
        while (reader.next()) {
            Assert.assertEquals(stringJoin(expect.get(i++)), stringJoin(reader.getRow()));
        }

        SnapshotTable difTable = snapshotManager.buildSnapshot(genTable("./dif", dif), genTableDesc("TEST_TABLE"),
                kylinConfig);
        Assert.assertNotEquals(origin.getUuid(), difTable.getUuid());
    }

    @Test
    public void testUseMd5deduplicateInV2() throws IOException {
        System.setProperty("kylin.snapshot.impl-class", "org.apache.kylin.dict.lookup.SnapshotTableV2");
        kylinConfig = KylinConfig.getInstanceFromEnv();
        snapshotManager = SnapshotManager.getInstance(kylinConfig);
        SnapshotTableV2 origin = (SnapshotTableV2) snapshotManager.buildSnapshot(genTable("./origin", expect),
                genTableDesc("TEST_TABLE"), kylinConfig);

        SnapshotTableV2 difTable = (SnapshotTableV2) snapshotManager.buildSnapshot(genTable("./dif", dif),
                genTableDesc("TEST_TABLE"), kylinConfig);
        Assert.assertNotEquals(origin.getUuid(), difTable.getUuid());
        Assert.assertNotEquals(origin.getMd5(), difTable.getMd5());

        difTable.setMD5(origin.getMd5());
        SnapshotTable actual = snapshotManager.trySaveNewSnapshot(difTable);

        SnapshotTable expect = snapshotManager.getSnapshotTable(origin.getResourcePath());
        Assert.assertEquals(origin.getUuid(), actual.getUuid());
        IReadableTable.TableReader expectReader = expect.getReader();
        IReadableTable.TableReader actualReader = actual.getReader();
        Assert.assertEquals(expect.getRowCount(), actual.getRowCount());
        while (expectReader.next() && actualReader.next()) {
            Assert.assertEquals(stringJoin(expectReader.getRow()), stringJoin(actualReader.getRow()));
        }
        Assert.assertFalse(expectReader.next());
        Assert.assertFalse(actualReader.next());

    }

    private String stringJoin(String[] strings) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < strings.length; i++) {
            builder.append(strings[i]);
            if (i < strings.length - 1) {
                builder.append(",");
            }
        }
        return builder.toString();
    }
}
