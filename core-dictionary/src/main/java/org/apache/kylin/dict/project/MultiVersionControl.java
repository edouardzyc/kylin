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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiVersionControl {
    public static final Logger logger = LoggerFactory.getLogger(MultiVersionControl.class);

    private String key;
    private ResourceLock.Lock lockInternal;
    private Semaphore semaphore = new Semaphore(1);
    private volatile AtomicLong id = new AtomicLong(-1);

    MultiVersionControl(String key, ResourceLock resourceLock) throws IOException, InterruptedException {
        this.key = key;
        ProjectDictionaryVersionInfo maxVersion = ProjectDictionaryManager.getInstance().getMaxVersion(key);
        if (maxVersion != null) {
            id.set(ProjectDictionaryManager.getInstance().getMaxVersion(key).getMaxVersion());
        }
        logger.info("acquire mvc lock for : " + key);
        lockInternal = resourceLock.getLockInterna(key);
    }

    public long getCurrentVersion() {
        return id.get();
    }
    private long getDictionaryVersion() {
        return getCurrentVersion() + 1;
    }

    VersionEntry beginAppendWhenPreviousAppendCompleted() {
        try {
            logger.info("acquire lock for : " + key);
            semaphore.acquire();

        } catch (InterruptedException e) {
            throw new RuntimeException("Interrupt with acquire lock", e);
        }
        logger.info("Get lock for : " + key + " version : " + getDictionaryVersion());

        return new VersionEntry(getDictionaryVersion());
    }

    void clear() {
        lockInternal.release();
    }

    void commit() {
        // First add  then release
        id.incrementAndGet();
        logger.info("release lock for : " + key + "  version: " + (id.get() + 1));
        semaphore.release();
    }

    class VersionEntry {
        private long version;

        VersionEntry(long version) {
            this.version = version;
        }

        public long getVersion() {
            return version;
        }

        public void setVersion(long version) {
            this.version = version;
        }
    }
}