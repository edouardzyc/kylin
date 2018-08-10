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

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class HiveMRInputTest {

    @Test
    @Ignore
    // Ignore this ut for method BatchCubingInputSide.materializeViewHql() depends on kylin config in environment
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
    }

}