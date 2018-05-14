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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionControl {
    public static final Logger logger = LoggerFactory.getLogger(VersionControl.class);

    private String key;
    private Semaphore lock;
    private Semaphore semaphore = new Semaphore(1);
    private volatile AtomicLong id = new AtomicLong(-1);

    VersionControl(String key) throws InterruptedException {
        this.key = key;
        ProjectDictionaryVersionInfo maxVersion = ProjectDictionaryManager.getInstance().getMaxVersion(key);
        if (maxVersion != null) {
            id.set(ProjectDictionaryManager.getInstance().getMaxVersion(key).getProjectDictionaryVersion());
        }
        logger.info("Acquire mvc lock for : " + key);
        lock = MVCLock.getLock(key);
    }

    public long getCurrentVersion() {
        return id.get();
    }

    private long getDictionaryVersion() {
        return getCurrentVersion() + 1;
    }

    long acquireMyVersion() {
        try {
            logger.info("Acquire lock for : " + key);
            semaphore.acquire();

        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted when acquiring lock for key " + key, e);
        }
        logger.info("Get lock for : " + key + " version : " + getDictionaryVersion());

        return getDictionaryVersion();
    }

    void clear() {
        MVCLock.remove(key);
        lock.release();
    }

    void commit(boolean isSuccess) {
        // First add  then release
        if (isSuccess) {
            id.incrementAndGet();
        }
        logger.info("release the lock : " + key + "  version: " + (id.get() + 1));
        semaphore.release();
    }

    public static class MVCLock {
        private static final ConcurrentHashMap<String, Semaphore> locks = new ConcurrentHashMap<>();

        public static Semaphore getLock(String sourcePath) throws InterruptedException {
            Semaphore semaphore = locks.get(sourcePath);
            if (semaphore == null) {
                synchronized (MVCLock.class) {
                    semaphore = locks.get(sourcePath);
                    if (semaphore == null) {
                        semaphore = new Semaphore(1);
                        semaphore.acquire();
                        locks.put(sourcePath, semaphore);
                        return semaphore;
                    } else {
                        semaphore.acquire();
                        return semaphore;
                    }
                }
            } else {
                logger.error("Find multiple VersionControl acquire lock.");
                throw new RuntimeException("Find multiple VersionControl acquire lock.");
            }
        }

        public static synchronized void remove(String sourcePath) {
            locks.remove(sourcePath);
        }
    }

}