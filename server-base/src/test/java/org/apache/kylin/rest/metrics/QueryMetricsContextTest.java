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
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

public class QueryMetricsContextTest extends LocalFileMetadataTestCase {

    private final String QUERY_ID = "3395dd9a-a8fb-47c0-b586-363271ca52e2";

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setup() {
        staticCreateTestMetadata();
    }

    @After
    public void teardown() {
        QueryMetricsContext.reset();
        staticCleanupTestMetadata();
    }

    @Test
    public void assertStart() {
        Assert.assertEquals(false, QueryMetricsContext.isStarted());

        QueryMetricsContext.start(QUERY_ID);
        Assert.assertEquals(false, QueryMetricsContext.isStarted());

        QueryMetricsContext.kylinConfig.setProperty("kap.metric.diagnosis.graph-writer-type", "INFLUX");
        QueryMetricsContext.start(QUERY_ID);
        Assert.assertEquals(true, QueryMetricsContext.isStarted());
    }

    @Test
    public void assertLogWithoutStart() {
        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("Query metric context is not started");

        QueryMetricsContext.log(RandomStringUtils.random(10));
    }

    @Test
    public void assertCollectWithoutStart() {
        exceptionRule.expect(IllegalStateException.class);
        exceptionRule.expectMessage("Query metric context is not started");

        QueryMetricsContext.collect(Mockito.mock(SQLRequest.class), Mockito.mock(SQLResponse.class),
                Mockito.mock(QueryContext.class));
    }
}
