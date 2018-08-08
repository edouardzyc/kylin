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

package org.apache.kylin.job.dao;

import org.apache.kylin.common.persistence.BrokenEntity;
import org.apache.kylin.common.persistence.BrokenInputStream;
import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class ExecutableOutputPOSerializerTest {

    ExecutableOutoutPOSerializer serializer;
    DataInputStream brokenIn;
    DataInputStream in;

    @Before
    public void setup() throws Exception {
        serializer = new ExecutableOutoutPOSerializer();
        ExecutableOutputPO mockOutput = new ExecutableOutputPO();
        mockOutput.setContent("test");
        mockOutput.setStatus("ERROR");
        brokenIn = new DataInputStream(new BrokenInputStream(new BrokenEntity("/testPath", "this is ErrorMsg")));
        in = new DataInputStream(new ByteArrayInputStream(JsonUtil.writeValueAsBytes(mockOutput)));
    }

    @After
    public void after() throws Exception {

    }

    @Test
    public void testDeserializeBrokenIn() throws IOException {
        ExecutableOutputPO entity = serializer.deserialize(brokenIn);
        assertEquals("ERROR", entity.getStatus());
        assertEquals("this is ErrorMsg", entity.getContent());
        assertEquals("testPath", entity.getUuid());
    }

    @Test
    public void testDeserializeIn() throws IOException {
        ExecutableOutputPO entity = serializer.deserialize(in);
        assertEquals("ERROR", entity.getStatus());
        assertEquals("test", entity.getContent());
    }

}
