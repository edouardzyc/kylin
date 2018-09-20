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

package org.apache.kylin.dict.lookup;

import java.io.IOException;
import java.util.NavigableSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.IReadableTable.TableSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * @author yangli9
 */
public class SnapshotManager {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotManager.class);

    public static SnapshotManager getInstance(KylinConfig config) {
        return config.getManager(SnapshotManager.class);
    }

    // called by reflection
    static SnapshotManager newInstance(KylinConfig config) throws IOException {
        return new SnapshotManager(config);
    }

    // ============================================================================

    private KylinConfig config;

    // path ==> SnapshotTable
    private LoadingCache<String, SnapshotTable> snapshotCache; // resource

    private SnapshotManager(KylinConfig config) {
        this.config = config;
        this.snapshotCache = CacheBuilder.newBuilder().removalListener(new RemovalListener<String, SnapshotTable>() {
            @Override
            public void onRemoval(RemovalNotification<String, SnapshotTable> notification) {
                SnapshotManager.logger.info("Snapshot with resource path " + notification.getKey()
                        + " is removed due to " + notification.getCause());
            }
        }).softValues().maximumSize(config.getCachedSnapshotMaxEntrySize())//
                .expireAfterWrite(1, TimeUnit.DAYS).build(new CacheLoader<String, SnapshotTable>() {
                    @Override
                    public SnapshotTable load(String key) throws Exception {
                        SnapshotTable snapshotTable = SnapshotManager.this.load(key, true);
                        return snapshotTable;
                    }
                });
    }

    public void wipeoutCache() {
        snapshotCache.invalidateAll();
    }

    public SnapshotTable getSnapshotTable(String resourcePath) throws IOException {
        try {
            SnapshotTable r = snapshotCache.get(resourcePath);
            if (r == null) {
                r = load(resourcePath, true);
                snapshotCache.put(resourcePath, r);
            }
            return r;
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    public void removeSnapshot(String resourcePath) throws IOException {
        ResourceStore store = getStore();
        store.deleteResource(resourcePath);
        snapshotCache.invalidate(resourcePath);
    }

    /**
     *
     * @param table source table
     * @param tableDesc table desc
     * @param cubeConfig cube config
     * @return Return snapshotTable is empty, if you need use it, please reload it by SnapshotManager#getSnapshotTable(java.lang.String).
     * @throws IOException
     */
    public SnapshotTable buildSnapshot(IReadableTable table, TableDesc tableDesc, KylinConfig cubeConfig)
            throws IOException {
        SnapshotTable snapshot = SnapshotTableFactory.create(table, tableDesc.getIdentity(), cubeConfig);
        snapshot.updateRandomUuid();

        String dup = checkDupByInfo(snapshot);
        if (dup != null) {
            logger.info("Identical input " + table.getSignature() + ", reuse existing snapshot at " + dup);
            return getSnapshotTable(dup);
        }

        if ((float) snapshot.getSignature().getSize() / 1024 / 1024 > cubeConfig.getTableSnapshotMaxMB()) {
            throw new IllegalStateException(
                    "Table snapshot should be no greater than " + cubeConfig.getTableSnapshotMaxMB() //
                            + " MB, but " + tableDesc + " size is " + snapshot.getSignature().getSize());
        }

        snapshot.init(table, tableDesc);

        return trySaveNewSnapshot(snapshot);
    }

    public SnapshotTable rebuildSnapshot(IReadableTable table, TableDesc tableDesc, String overwriteUUID,
            KylinConfig conf) throws IOException {
        SnapshotTable snapshot = SnapshotTableFactory.create(table, tableDesc.getIdentity(), conf);
        snapshot.setUuid(overwriteUUID);

        snapshot.init(table, tableDesc);

        SnapshotTable existing = getSnapshotTable(snapshot.getResourcePath());
        snapshot.setLastModified(existing.getLastModified());

        save(snapshot);

        return snapshot;
    }

    public SnapshotTable trySaveNewSnapshot(SnapshotTable snapshotTable) throws IOException {

        String dupTable = checkDupByContent(snapshotTable);
        if (dupTable != null) {
            logger.info("Identical snapshot content " + snapshotTable + ", reuse existing snapshot at " + dupTable);
            return getSnapshotTable(dupTable);
        }

        save(snapshotTable);

        return snapshotTable;
    }

    private String checkDupByInfo(SnapshotTable snapshot) throws IOException {
        ResourceStore store = getStore();
        String resourceDir = snapshot.getResourceDir();
        NavigableSet<String> existings = store.listResources(resourceDir);
        if (existings == null)
            return null;

        TableSignature sig = snapshot.getSignature();
        for (String existing : existings) {
            SnapshotTable existingTable = load(existing, false);
            if (existingTable != null && sig.equals(existingTable.getSignature()))
                if (snapshot.getClass().equals(existingTable.getClass()))
                    return existing;
        }

        return null;
    }

    private String checkDupByContent(SnapshotTable snapshot) throws IOException {
        ResourceStore store = getStore();
        String resourceDir = snapshot.getResourceDir();
        NavigableSet<String> existings = store.listResources(resourceDir);
        if (existings == null)
            return null;

        // SnapshotTableV1 need data to compare row by row, SnapshotTableV2 compare by info
        boolean loadData = false;
        if (snapshot instanceof SnapshotTableV1) {
            loadData = true;
        }
        for (String existing : existings) {
            SnapshotTable existingTable = load(existing, loadData);
            if (existingTable != null && existingTable.equals(snapshot))
                return existing;
        }

        return null;
    }

    private void save(SnapshotTable snapshot) throws IOException {
        ResourceStore store = getStore();
        String path = snapshot.getResourcePath();
        store.putResourceStreaming(path, snapshot, SnapshotTableSerializer.FULL_SERIALIZER);
    }

    private SnapshotTable load(String resourcePath, boolean loadData) throws IOException {
        logger.info("Loading snapshotTable from " + resourcePath + ", with loadData: " + loadData);
        ResourceStore store = getStore();

        SnapshotTable table = store.getResource(resourcePath, SnapshotTable.class,
                loadData ? SnapshotTableSerializer.FULL_SERIALIZER : SnapshotTableSerializer.INFO_SERIALIZER);

        if (loadData)
            logger.debug("Loaded snapshot at " + resourcePath);

        return table;
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }
}
