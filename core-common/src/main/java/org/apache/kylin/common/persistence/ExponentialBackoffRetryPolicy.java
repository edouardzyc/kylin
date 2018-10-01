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

package org.apache.kylin.common.persistence;

public class ExponentialBackoffRetryPolicy {
    private final int baseSleepTimeMs;
    private final int maxSleepTimeMs;
    private long firstSleepTime;
    private int retryCount;

    public ExponentialBackoffRetryPolicy(int baseSleepTimeMs, int maxSleepTimeMs) {
        this.baseSleepTimeMs = baseSleepTimeMs;
        this.maxSleepTimeMs = maxSleepTimeMs;
        this.retryCount = 0;
    }

    long getSleepTimeMs() {
        if (retryCount == 0)
            firstSleepTime = System.currentTimeMillis();

        long ms = baseSleepTimeMs * (1 << retryCount);

        if (ms > maxSleepTimeMs)
            ms = maxSleepTimeMs;
        return ms;
    }

    void increaseRetryCount() {
        retryCount++;
    }

    boolean isTimeOut(long timeoutMs) {
        return retryCount != 0 && (System.currentTimeMillis() - firstSleepTime >= timeoutMs);
    }
}
