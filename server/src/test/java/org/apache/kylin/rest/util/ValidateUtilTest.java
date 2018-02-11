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

package org.apache.kylin.rest.util;

import java.io.IOException;

import org.apache.kylin.rest.service.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

public class ValidateUtilTest extends ServiceTestBase {
    @Autowired
    @Qualifier("validateUtil")
    private ValidateUtil validateUtil;

    @Test
    public void testCheckIdentifiersExists() throws IOException {
        validateUtil.checkIdentifiersExists("ADMIN", true);
        validateUtil.checkIdentifiersExists("ROLE_ADMIN", false);

        try {
            validateUtil.checkIdentifiersExists("USER_NOT_EXITS", true);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Operation failed, user:USER_NOT_EXITS not exists, please add first.", e.getMessage());
        }

        try {
            validateUtil.checkIdentifiersExists("ROLE_NOT_EXITS", false);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Operation failed, group:ROLE_NOT_EXITS not exists, please add first.", e.getMessage());
        }
    }
}
