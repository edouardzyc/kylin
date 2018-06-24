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

package org.apache.kylin.metadata.suite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class SuiteInfoManager {
    private static final Logger logger = LoggerFactory.getLogger(SuiteInfoManager.class);

    public static SuiteInfoManager getInstance(KylinConfig config) {
        return config.getManager(SuiteInfoManager.class);
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.kylinConfig);
    }

    // called by reflection
    static SuiteInfoManager newInstance(KylinConfig config) throws IOException {
        return new SuiteInfoManager(config);
    }

    private KylinConfig kylinConfig;

    // suite id ==> SuiteInfoInstance
    private CaseInsensitiveStringCache<SuiteInfoInstance> suiteInfoInstanceMap;
    private CachedCrudAssist<SuiteInfoInstance> crud;
    private AutoReadWriteLock lock = new AutoReadWriteLock();

    private SuiteInfoManager(KylinConfig config) throws IOException {
        logger.info("Initializing SuiteInfoManager with config " + config);
        this.kylinConfig = config;
        this.suiteInfoInstanceMap = new CaseInsensitiveStringCache<>(config, "suite_info");
        this.crud = new CachedCrudAssist<SuiteInfoInstance>(getStore(), "/suite_info", "", SuiteInfoInstance.class,
                suiteInfoInstanceMap, true) {
            @Override
            protected SuiteInfoInstance initEntityAfterReload(SuiteInfoInstance suiteInfo, String resourceName) {
                return suiteInfo;
            }
        };

        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new SuiteInfoSyncListener(), "suite_info");
    }

    private class SuiteInfoSyncListener extends Broadcaster.Listener {
        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            try (AutoLock l = lock.lockForWrite()) {
                if (event == Event.DROP)
                    suiteInfoInstanceMap.removeLocal(cacheKey);
                else
                    crud.reloadQuietly(cacheKey);
            }
        }
    }

    public List<SuiteInfoInstance> listAllSuiteInfos() {
        try (AutoLock l = lock.lockForRead()) {
            return Lists.newArrayList(suiteInfoInstanceMap.values());
        }
    }

    public SuiteInfoInstance getSuiteInfo(String suiteId) {
        try (AutoLock l = lock.lockForRead()) {
            return suiteInfoInstanceMap.get(suiteId);
        }
    }

    public SuiteInfoInstance saveSuite(SuiteInfoInstance suiteInfo, boolean updateIfExist) {
        try (AutoLock l = lock.lockForWrite()) {
            if (suiteInfo == null || suiteInfo.getSuiteId() == null) {
                throw new IllegalArgumentException("Invalid suite.");
            }
            SuiteInfoInstance exist = suiteInfoInstanceMap.get(suiteInfo.getSuiteId());

            if (exist != null) {
                if (updateIfExist) {
                    suiteInfo.setLastModified(exist.getLastModified());
                } else {
                    return exist;
                }
            }
            if (suiteInfo.getProjects() == null) {
                suiteInfo.setProjects(new ArrayList<String>());
            }
            return crud.save(suiteInfo);
        } catch (IOException e) {
            throw new RuntimeException("Can not save suite info.", e);
        }
    }

    public SuiteInfoInstance addProjectToSuite(String suiteId, String projectName) {
        try (AutoLock l = lock.lockForWrite()) {
            SuiteInfoInstance exist = suiteInfoInstanceMap.get(suiteId);
            if (exist == null) {
                throw new IllegalArgumentException("Suite does not exist with id: " + suiteId);
            }
            exist.addProject(projectName);
            return saveSuite(exist, true);
        }
    }

    public void removeSuiteInfo(String suiteId) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            crud.delete(suiteId);
        }
    }
}
