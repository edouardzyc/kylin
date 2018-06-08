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
package org.apache.kylin.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class JoinsGraphTest extends LocalFileMetadataTestCase {
    private DataModelDesc modelDesc;
    private Map<String, TableRef> name2tblMap = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        modelDesc = DataModelManager.getInstance(getTestConfig()).getDataModelDesc("ci_inner_join_model");
        for (TableRef tableRef : modelDesc.getAllTables())
            name2tblMap.put(tableRef.getAlias().toUpperCase(), tableRef);
    }

    @Test
    public void match() throws Exception {
        JoinsGraph modelJoinsGraph = modelDesc.getJoinsGraph();
        JoinsGraph queryGraphOfSingleTbl = new JoinsGraph(name2tblMap.get("TEST_KYLIN_FACT"),
                Lists.<JoinDesc> newArrayList());
        Assert.assertTrue(JoinsGraph.match(modelJoinsGraph, modelJoinsGraph, new HashMap<String, String>()));
        Assert.assertTrue(JoinsGraph.match(queryGraphOfSingleTbl, modelJoinsGraph, new HashMap<String, String>()));
        Assert.assertTrue(
                JoinsGraph.match(queryGraphOfSingleTbl, queryGraphOfSingleTbl, new HashMap<String, String>()));
        Assert.assertFalse(JoinsGraph.match(modelJoinsGraph, queryGraphOfSingleTbl, new HashMap<String, String>()));

        JoinsGraph queryGraphOf2Tbls = new JoinsGraph(name2tblMap.get("TEST_ORDER"),
                Lists.<JoinDesc> newArrayList(mockJoinDesc("INNER", new String[] { "TEST_KYLIN_FACT.ORDER_ID" },
                        new String[] { "TEST_ORDER.ORDER_ID" })));
        JoinsGraph queryGraphOf2Tbls1 = new JoinsGraph(name2tblMap.get("TEST_KYLIN_FACT"),
                Lists.<JoinDesc> newArrayList(mockJoinDesc("INNER", new String[] { "TEST_KYLIN_FACT.ORDER_ID" },
                        new String[] { "TEST_ORDER.ORDER_ID" })));
        Assert.assertTrue(JoinsGraph.match(queryGraphOf2Tbls, queryGraphOf2Tbls1, new HashMap<String, String>()));

        JoinsGraph graphWithoutFactTbl = new JoinsGraph(name2tblMap.get("TEST_ORDER"),
                Lists.<JoinDesc> newArrayList(mockJoinDesc("INNER", new String[] { "TEST_ORDER.BUYER_ID" },
                        new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" })));
        Assert.assertFalse(JoinsGraph.match(graphWithoutFactTbl, modelJoinsGraph, new HashMap<String, String>()));
        List<JoinDesc> joinDescs = Lists.newArrayList();
        joinDescs.add(mockJoinDesc("INNER", new String[] { "TEST_ORDER.BUYER_ID" },
                new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }));
        joinDescs.add(mockJoinDesc("INNER", new String[] { "BUYER_ACCOUNT.ACCOUNT_COUNTRY" },
                new String[] { "BUYER_COUNTRY.COUNTRY" }));
        JoinsGraph graphWithoutFactTbl3 = new JoinsGraph(name2tblMap.get("TEST_ORDER"), joinDescs);
        Assert.assertFalse(JoinsGraph.match(graphWithoutFactTbl3, modelJoinsGraph, new HashMap<String, String>()));

        joinDescs.clear();
        joinDescs.add(mockJoinDesc("INNER", new String[] { "TEST_ORDER.BUYER_ID" },
                new String[] { "BUYER_ACCOUNT.ACCOUNT_ID" }));
        joinDescs.add(mockJoinDesc("INNER", new String[] { "TEST_KYLIN_FACT.ORDER_ID" },
                new String[] { "TEST_ORDER.ORDER_ID" }));
        JoinsGraph queryGraphOf3Tbl = new JoinsGraph(name2tblMap.get("TEST_ORDER"), joinDescs);
        Assert.assertTrue(JoinsGraph.match(queryGraphOf3Tbl, modelJoinsGraph, new HashMap<String, String>()));

    }

    private JoinDesc mockJoinDesc(String joinType, String[] fkCols, String[] pkCols) {
        JoinDesc joinDesc = new JoinDesc();
        joinDesc.setType(joinType);
        joinDesc.setPrimaryKey(fkCols);
        joinDesc.setPrimaryKey(pkCols);
        TblColRef[] fkColRefs = new TblColRef[fkCols.length];
        for (int i = 0; i < fkCols.length; i++) {
            fkColRefs[i] = modelDesc.findColumn(fkCols[i]);
        }
        TblColRef[] pkColRefs = new TblColRef[pkCols.length];
        for (int i = 0; i < pkCols.length; i++) {
            pkColRefs[i] = modelDesc.findColumn(pkCols[i]);
        }
        joinDesc.setForeignKeyColumns(fkColRefs);
        joinDesc.setPrimaryKeyColumns(pkColRefs);
        return joinDesc;
    }

}