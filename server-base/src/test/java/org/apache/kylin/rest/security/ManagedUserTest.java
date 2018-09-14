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

package org.apache.kylin.rest.security;

import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

public class ManagedUserTest {
    @Test
    public void testAuthenticateFail() {
        LinkedList<SimpleGrantedAuthority> auth = new LinkedList<>();
        ManagedUser adminUser = new ManagedUser("ADMIN", "KYLIN", auth, false, false, false, 0, 0, 0);
        Assert.assertTrue(adminUser.getFirstLoginFailedTime() == 0);

        adminUser.authenticateFail(); //User "ADMIN" login failed for the first time
        long firstLoginFailedTime = adminUser.getFirstLoginFailedTime();
        Assert.assertTrue(adminUser.getFirstLoginFailedTime() > 0);
        Assert.assertTrue(adminUser.getWrongTime() == 1);
        Assert.assertTrue(!adminUser.isLocked());
        Assert.assertTrue(adminUser.getLockedTime() == 0);

        adminUser.authenticateFail(); //User "ADMIN" login failed for the second time
        Assert.assertTrue(adminUser.getFirstLoginFailedTime() == firstLoginFailedTime);
        Assert.assertTrue(adminUser.getWrongTime() == 2);
        Assert.assertTrue(!adminUser.isLocked());
        Assert.assertTrue(adminUser.getLockedTime() == 0);

        adminUser.authenticateFail(); //User "ADMIN" login failed for the third time
        Assert.assertTrue(adminUser.getFirstLoginFailedTime() == firstLoginFailedTime);
        Assert.assertTrue(adminUser.getWrongTime() == 3);
        Assert.assertTrue(adminUser.isLocked());
        Assert.assertTrue(adminUser.getLockedTime() > 0);

        adminUser.authenticateFail(); //User "ADMIN" login failed for the forth time
        Assert.assertTrue(adminUser.getFirstLoginFailedTime() == firstLoginFailedTime);
        Assert.assertTrue(adminUser.getWrongTime() == 4);
        Assert.assertTrue(adminUser.isLocked());
        Assert.assertTrue(adminUser.getLockedTime() > 0);

        //user "TEST" first login Failed 15 mins ago
        long userTestFirstFailedTime = System.currentTimeMillis() - (16 * 60 * 1000 + 1);
        ManagedUser testUser = new ManagedUser("TEST", "KYLIN", auth, false, false, true, System.currentTimeMillis(), 4,
                userTestFirstFailedTime);
        testUser.authenticateFail();
        Assert.assertTrue(testUser.getFirstLoginFailedTime() > userTestFirstFailedTime);
        Assert.assertTrue(testUser.getWrongTime() == 1);
        Assert.assertTrue(!testUser.isLocked());
        Assert.assertTrue(testUser.getLockedTime() == 0);
    }

    @Test
    public void testClearAuthenticateFailedRecord() {
        LinkedList<SimpleGrantedAuthority> auth = new LinkedList<>();
        ManagedUser managedUser = new ManagedUser("ADMIN", "KYLIN", auth, false, false, true,
                System.currentTimeMillis(), 2, System.currentTimeMillis());

        managedUser.clearAuthenticateFailedRecord();
        Assert.assertTrue(managedUser.getWrongTime() == 0);
        Assert.assertTrue(managedUser.getFirstLoginFailedTime() == 0);
        Assert.assertTrue(managedUser.getLockedTime() == 0);
        Assert.assertTrue(!managedUser.isLocked());
    }
}
