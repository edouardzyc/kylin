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

import java.math.BigDecimal;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExpressionParamTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void getValueOf() throws Exception {
        String[] oprands = new String[] { "*", "1", "1.2" };
        assertEquals(new BigDecimal(1.2).doubleValue(),
                new ParameterDesc.ExpressionParam("*", false).getValueOf(oprands).doubleValue(), 0.001);

        oprands = new String[] { "*", "1.2", "1.2" };
        assertEquals(new BigDecimal(1.44).doubleValue(),
                new ParameterDesc.ExpressionParam("*", false).getValueOf(oprands).doubleValue(), 0.001);

        oprands = new String[] { "*", "1000000", "10000" };
        assertEquals(new BigDecimal("10000000000").longValue(),
                new ParameterDesc.ExpressionParam("*", false).getValueOf(oprands).longValue(), 0.001);
    }

    @Test
    public void testDerivedDataType() {
        assertEquals("decimal(19,4)",
                ParameterDesc.ExpressionParam.deriveReturnType("*", "decimal", "decimal(19,2)").toString());
        assertEquals("decimal(19,6)",
                ParameterDesc.ExpressionParam.deriveReturnType("*", "decimal(19, 6)", "decimal(19,4)").toString());
        assertEquals("decimal(19,4)",
                ParameterDesc.ExpressionParam.deriveReturnType("*", "decimal", "decimal").toString());

        assertEquals("bigint", ParameterDesc.ExpressionParam.deriveReturnType("*", "bigint", "smallint").toString());
        assertEquals("bigint", ParameterDesc.ExpressionParam.deriveReturnType("*", "bigint", "integer").toString());

        assertEquals("decimal(19,4)",
                ParameterDesc.ExpressionParam.deriveReturnType("*", "decimal", "bigint").toString());
        assertEquals("decimal(19,4)",
                ParameterDesc.ExpressionParam.deriveReturnType("*", "decimal", "double").toString());

        assertEquals("double", ParameterDesc.ExpressionParam.deriveReturnType("*", "bigint", "double").toString());
        assertEquals("double", ParameterDesc.ExpressionParam.deriveReturnType("*", "integer", "double").toString());
        assertEquals("double", ParameterDesc.ExpressionParam.deriveReturnType("*", "double", "double").toString());

    }

}