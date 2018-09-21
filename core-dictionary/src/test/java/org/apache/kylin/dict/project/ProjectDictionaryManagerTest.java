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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.MockupReadableTable;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.cache.LoadingCache;

public class ProjectDictionaryManagerTest extends LocalFileMetadataTestCase {

    ProjectDictionaryManager projectDictionaryManager;

    @Before
    public void before() throws Exception {
        staticCreateTestMetadata();
        projectDictionaryManager = ProjectDictionaryManager.getInstance();
        //TODO ugly
        projectDictionaryManager.clear();
    }

    @Test
    public void testDictCache() throws Exception {
        String project = "test";
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        DataModelManager metaMgr = DataModelManager.getInstance(config);
        DataModelDesc model = metaMgr.getDataModelDesc("test_kylin_inner_join_model_desc");
        TblColRef col = model.findColumn("lstg_format_name");

        DictionaryInfo dictionaryInfo = dictMgr.buildDictionary(col,
                MockupReadableTable.newSingleColumnTable("/a/path", "1", "2", "3"));
        SegProjectDict segProjectDict = projectDictionaryManager.append(project, dictionaryInfo);
        dictMgr.removeDictionary(dictionaryInfo.getResourcePath());
        dictionaryInfo = null;

        String sourceIdentifier = segProjectDict.getSourceIdentifier();
        String dictResPath = ProjectDictionaryHelper.PathBuilder.dataPath(sourceIdentifier,
                segProjectDict.getCurrentVersion());

        ProjectDictionaryInfo projectDictionaryInfo = projectDictionaryManager.getDictionary(dictResPath);
        Dictionary dictionary = projectDictionaryInfo.getDictionaryObject();

        // 1. clean cache
        releaseDictionary(projectDictionaryManager, dictResPath);

        Dictionary dictionary2 = getDictObjByWeakCache(dictResPath);
        assertNotNull(dictionary2);

        ProjectDictionaryInfo projectDictionaryInfo2 = getDictInfoByWeakCache(dictResPath);
        assertNotNull(projectDictionaryInfo2);

        // 2. gc
        System.gc();
        Thread.sleep(3000);

        ProjectDictionaryInfo projectDictionaryInfo3 = getDictInfoByWeakCache(dictResPath);
        assertNotNull(projectDictionaryInfo3);

        Dictionary dictionary3 = getDictObjByWeakCache(dictResPath);
        assertNotNull(dictionary3);

        ProjectDictionaryInfo projectDictionaryInfo4 = projectDictionaryManager.getDictionary(dictResPath);
        assertNotNull(projectDictionaryInfo4);
        assertEquals(projectDictionaryInfo, projectDictionaryInfo4);

        // 3. clean cache && clean projectDictionaryInfo handle && gc
        releaseDictionary(projectDictionaryManager, dictResPath);
        projectDictionaryInfo = null;
        projectDictionaryInfo2 = null;
        projectDictionaryInfo3 = null;
        projectDictionaryInfo4 = null;
        System.gc();
        Thread.sleep(3000);

        ProjectDictionaryInfo projectDictionaryInfo5 = getDictInfoByWeakCache(dictResPath);
        assertNull(projectDictionaryInfo5);

        Dictionary dictionary4 = getDictObjByWeakCache(dictResPath);
        assertNotNull(dictionary4);

        ProjectDictionaryInfo projectDictionaryInfo6 = projectDictionaryManager.getDictionary(dictResPath);
        assertNotNull(projectDictionaryInfo6);

        // 4. clean cache && clean all handle && gc
        releaseDictionary(projectDictionaryManager, dictResPath);
        dictionary = null;
        dictionary2 = null;
        dictionary3 = null;
        dictionary4 = null;
        projectDictionaryInfo5 = null;
        projectDictionaryInfo6 = null;
        System.gc();
        Thread.sleep(3000);

        Dictionary dictionary5 = getDictObjByWeakCache(dictResPath);
        assertNull(dictionary5);

        ProjectDictionaryInfo projectDictionaryInfo7 = getDictInfoByWeakCache(dictResPath);
        assertNull(projectDictionaryInfo7);

        ProjectDictionaryInfo projectDictionaryInfo8 = projectDictionaryManager.getDictionary(dictResPath);
        assertNotNull(projectDictionaryInfo8);

    }

    @Test
    public void testClearAllListener() throws Exception {
        String project = "test";
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        DataModelManager metaMgr = DataModelManager.getInstance(config);
        DataModelDesc model = metaMgr.getDataModelDesc("test_kylin_inner_join_model_desc");
        TblColRef col = model.findColumn("lstg_format_name");

        DictionaryInfo dictionaryInfo = dictMgr.buildDictionary(col,
                MockupReadableTable.newSingleColumnTable("/a/path", "1", "2", "3"));
        SegProjectDict segProjectDict = projectDictionaryManager.append(project, dictionaryInfo);
        ProjectDictionaryVersionInfo versionInfo = projectDictionaryManager.getMaxVersion(segProjectDict);

        ResourceStore resourceStore = ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
        JsonSerializer<ProjectDictionaryVersionInfo> serializer = new JsonSerializer<>(
                ProjectDictionaryVersionInfo.class);
        ProjectDictionaryVersionInfo resource = resourceStore
                .getResource(ResourceStore.PROJECT_DICT_VERSION_RESOURCE_ROOT + "/" + versionInfo.getKey()
                        + MetadataConstants.TYPE_VERSION, ProjectDictionaryVersionInfo.class, serializer);
        long newTs = System.currentTimeMillis() / 1000 * 1000 + 3000;
        resourceStore.putResource(ResourceStore.PROJECT_DICT_VERSION_RESOURCE_ROOT + "/" + versionInfo.getKey()
                + MetadataConstants.TYPE_VERSION, resource, newTs, serializer);

        ProjectDictionaryVersionInfo newInfo1 = projectDictionaryManager.getMaxVersion(segProjectDict);
        Assert.assertNotEquals(newTs, newInfo1.getLastModified());
        Broadcaster.getInstance(config).notifyListener("all", Broadcaster.Event.UPDATE, "all");
        ProjectDictionaryVersionInfo newInfo2 = projectDictionaryManager.getMaxVersion(segProjectDict);
        Assert.assertEquals(newTs, newInfo2.getLastModified());

    }

    private void releaseDictionary(ProjectDictionaryManager projectDictionaryManager, String dictResPath)
            throws NoSuchFieldException, IllegalAccessException {
        getDictCache(projectDictionaryManager).invalidate(dictResPath);
    }

    private ProjectDictionaryInfo getDictInfoByWeakCache(String resourcePath) throws Exception {
        Map<String, WeakReference<Object>> weakCache = getWeakCache(projectDictionaryManager);

        WeakReference reference = weakCache.get(resourcePath);
        if (reference == null) {
            return null;
        }
        return (ProjectDictionaryInfo) reference.get();
    }

    private Dictionary getDictObjByWeakCache(String resourcePath) throws Exception {
        Map<String, WeakReference<Object>> weakCache = getWeakCache(projectDictionaryManager);

        WeakReference reference = weakCache.get(ProjectDictionaryManager.dictObjKey(resourcePath));
        if (reference == null) {
            return null;
        }
        return (Dictionary) reference.get();
    }

    private Map<String, WeakReference<Object>> getWeakCache(ProjectDictionaryManager dictMgr)
            throws NoSuchFieldException, IllegalAccessException {
        Class<?> clazz = dictMgr.getClass();
        Field field = clazz.getDeclaredField("weakDictCache");
        field.setAccessible(true);
        return (Map<String, WeakReference<Object>>) field.get(dictMgr);
    }

    private LoadingCache<String, ProjectDictionaryInfo> getDictCache(ProjectDictionaryManager dictMgr)
            throws NoSuchFieldException, IllegalAccessException {
        Class<?> clazz = dictMgr.getClass();
        Field field = clazz.getDeclaredField("dictionaryInfoCache");
        field.setAccessible(true);
        return (LoadingCache<String, ProjectDictionaryInfo>) field.get(dictMgr);
    }
}
