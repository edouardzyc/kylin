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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.TableMetadataManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ColumnDescTest extends LocalFileMetadataTestCase {
    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void clean() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testIsColumnCompatible() {
        TableMetadataManager tblMgr = TableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        TableDesc tableDesc = tblMgr.getTableDesc("TEST_ACCOUNT", "default");
        ColumnDesc[] columns = tableDesc.getColumns();
        ColumnDesc column1 = columns[0]; //column[0].getType() is "bigint"
        ColumnDesc column2 = new ColumnDesc();
        column2.setName(column1.getName());
        column2.setType(columns[3].getType()); //columns[3].getType() is "string"
        Assert.assertTrue(!column1.isColumnCompatible(column2));

        column2.setName(column1.getName());
        column2.setType(columns[0].getType());
        Assert.assertTrue(column1.isColumnCompatible(column2));
    }
}
