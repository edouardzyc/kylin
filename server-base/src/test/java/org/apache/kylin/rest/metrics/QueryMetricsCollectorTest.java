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

package org.apache.kylin.rest.metrics;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

public class QueryMetricsCollectorTest {

    private final String QUERY_ID = "3395dd9a-a8fb-47c0-b586-363271ca52e2";

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @After
    public void teardown() {
        QueryMetricsCollector.reset();
    }

    @Test
    public void assertStart() {
        Assert.assertEquals(false, QueryMetricsCollector.isStarted());

        QueryMetricsCollector.start(QUERY_ID);

        Assert.assertEquals(true, QueryMetricsCollector.isStarted());

        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("Query metric collector already started");

        QueryMetricsCollector.start(QUERY_ID);
    }

    @Test
    public void assertLogWithoutStart() {
        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("Query metric collector is not started");

        QueryMetricsCollector.log(RandomStringUtils.random(10));
    }

    @Test
    public void assertCollectWithoutStart() {
        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("Query metric collector is not started");

        QueryMetricsCollector.collect(Mockito.mock(SQLRequest.class), Mockito.mock(SQLResponse.class),
                Mockito.mock(QueryContext.class));
    }
}
