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
import java.util.LinkedList;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiVersionControl {
    public static final Logger logger = LoggerFactory.getLogger(MultiVersionControl.class);

    private String key;
    private final LinkedList<VersionEntry> writeQueue = new LinkedList<>();
    private AtomicLong sequenceId;
    //    private DistributedLock distributedLock;
    private static final String VERSION_PREFIX = "version_";
    private DistributedLock distributedLock;

    MultiVersionControl(String key){
        this.key = key;
        distributedLock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory()
                .lockForCurrentThread();
        distributedLock.lock(key, Long.MAX_VALUE);
        ProjectDictionaryVersionInfo projectDictionaryVersion = ProjectDictionaryManager.getInstance().getMaxVersion(key);
        if (projectDictionaryVersion == null)
            this.sequenceId = new AtomicLong(-1);
        else
            this.sequenceId = new AtomicLong(projectDictionaryVersion.getMaxVersion());
        logger.info("init mvc:" + key + ": " + sequenceId.get());
    }

    @Deprecated
    Long[] listAllVersionWithHDFSs(FileSystem fileSystem, Path basePath) throws IOException {
        FileStatus[] versionDirs = fileSystem.listStatus(basePath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(VERSION_PREFIX);
            }
        });
        TreeSet<Long> versions = new TreeSet<>();
        for (FileStatus versionDir : versionDirs) {
            Path path = versionDir.getPath();
            versions.add(Long.parseLong(path.getName().substring(VERSION_PREFIX.length())));
        }
        return versions.toArray(new Long[0]);
    }

    public long getCurrentVersion() {

        return sequenceId.get();
    }

    private long getDictionaryVersion() {
        return sequenceId.incrementAndGet();
    }

    private boolean checkMyLock() {
        return distributedLock.isLockedByMe(key);
    }

    VersionEntry beginAppendWhenPreviousAppendCompleted() {
        Preconditions.checkState(checkMyLock(), "Current node is not job node");
        waitForPreviousAppendCompleted();
        // Ensure this is job node
        Preconditions.checkState(checkMyLock(), "Current node is not job node");
        return beginAppend(getDictionaryVersion());
    }

    private VersionEntry beginAppend(Long sequeueId) {
        VersionEntry e = new VersionEntry(sequeueId);
        synchronized (writeQueue) {
            writeQueue.add(e);
            return e;
        }
    }

    private void waitForPreviousAppendCompleted() {
        // create a null entity
        VersionEntry versionEntry = beginAppend(-1L);
        waitForPreviousAppendCompleted(versionEntry);
    }

    private void waitForPreviousAppendCompleted(VersionEntry waitedEntry) {
        logger.info("Request lock with column: " + key);
        // avend
        boolean interrupted = false;
        VersionEntry w = waitedEntry;

        try {
            VersionEntry firstEntry = null;
            do {
                synchronized (writeQueue) {
                    // writeQueue won't be empty at this point, the following is just a safety check
                    if (writeQueue.isEmpty()) {
                        break;
                    }
                    firstEntry = writeQueue.getFirst();
                    if (firstEntry == w) {
                        // all previous in-flight transactions are done
                        break;
                    }
                    try {
                        writeQueue.wait(0);
                    } catch (InterruptedException ie) {
                        // We were interrupted... finish the loop -- i.e. cleanup --and then
                        // on our way out, reset the interrupt flag.
                        interrupted = true;
                        break;
                    }
                }
            } while (firstEntry != null);
        } finally {
            if (w != null) {
                commit(w);
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    void rollBack(VersionEntry e) {
        synchronized (writeQueue) {
            e.markCompleted();

            while (!writeQueue.isEmpty()) {
                VersionEntry queueFirst = writeQueue.getFirst();
                if (queueFirst.isCompleted()) {
                    // Using Max because Edit complete in WAL sync order not arriving order
                    writeQueue.removeFirst();
                    sequenceId.decrementAndGet();
                } else {
                    break;
                }
            }

            // notify waiters on writeQueue before return
            writeQueue.notifyAll();
        }
    }

    void clear() {
        distributedLock.unlock(key);
    }

    void notifyAllThread(){
        writeQueue.notifyAll();
    }
    void commit(VersionEntry e) {
        synchronized (writeQueue) {
            e.markCompleted();

            while (!writeQueue.isEmpty()) {
                VersionEntry queueFirst = writeQueue.getFirst();
                if (queueFirst.isCompleted()) {
                    // Using Max because Edit complete in WAL sync order not arriving order
                    writeQueue.removeFirst();
                } else {
                    break;
                }
            }
            // notify waiters on writeQueue before return
            writeQueue.notifyAll();
        }

    }

    class VersionEntry {
        private long version;
        private boolean isCompleted = false;

        VersionEntry(long version) {
            this.version = version;
        }

        public long getVersion() {
            return version;
        }

        public void setVersion(long version) {
            this.version = version;
        }

        public boolean isCompleted() {
            return isCompleted;
        }

        void markCompleted() {
            this.isCompleted = true;
        }
    }
}
