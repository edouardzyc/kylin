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

package org.apache.kylin.dict.project;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

public class ResourceLock {
    private final ConcurrentHashMap<String, LockContext> lockedResources = new ConcurrentHashMap<>();
    private Long rowLockWaitDuration = 10000L; //todo config

    public Lock getLockInterna(String sourcePath) throws IOException, InterruptedException {
        LockContext lockContext = new LockContext(sourcePath);

        while (true) {
            LockContext existingContext = lockedResources.putIfAbsent(sourcePath, lockContext);
            if (existingContext == null) {
                // Row is not already locked by any thread, use newly created context.
                break;
            } else if (existingContext.ownedByCurrentThread()) {
                // Row is already locked by current thread, reuse existing context instead.
                lockContext = existingContext;
                break;
            } else {
                // Row is already locked by some other thread, give up or wait for it
                if (!existingContext.latch.await(this.rowLockWaitDuration, TimeUnit.MILLISECONDS)) {
                    throw new IOException("Timed out waiting for lock for row: " + sourcePath);
                }
            }
        }

        // allocate new lock for this thread
        return lockContext.newLock();
    }

    class LockContext {
        private final String sourcePath;
        private final CountDownLatch latch = new CountDownLatch(1);
        private final Thread thread;
        private int lockCount = 0;

        LockContext(String sourcePath) {
            this.sourcePath = sourcePath;
            this.thread = Thread.currentThread();
        }

        boolean ownedByCurrentThread() {
            return thread == Thread.currentThread();
        }

        Lock newLock() {
            lockCount++;
            return new Lock(this);
        }

        void releaseLock() {
            lockCount--;
            if (lockCount == 0) {
                // no remaining locks by the thread, unlock and allow other threads to access
                LockContext existingContext = lockedResources.remove(sourcePath);
                if (existingContext != this) {
                    throw new RuntimeException(
                            "Internal sourcePath lock state inconsistent, should not happen, sourcePath: "
                                    + sourcePath);
                }
                latch.countDown();
            }
        }
    }

    public static class Lock {
        @VisibleForTesting
        final LockContext context;
        private boolean released = false;

        @VisibleForTesting
        Lock(LockContext context) {
            this.context = context;
        }

        /**
         * Release the given lock.  If there are no remaining locks held by the current thread
         * then unlock the sourcePath and allow other threads to acquire the lock.
         *
         * @throws IllegalArgumentException if called by a different thread than the lock owning thread
         */
        public void release() {
            if (!released) {
                context.releaseLock();
                released = true;
            }
        }
    }
}
