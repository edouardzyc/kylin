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

package org.apache.kylin.dict;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.Map;

import com.google.common.cache.LoadingCache;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DictionaryManagerTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBuildSaveDictionary() throws IOException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        DataModelManager metaMgr = DataModelManager.getInstance(config);
        DataModelDesc model = metaMgr.getDataModelDesc("test_kylin_inner_join_model_desc");
        TblColRef col = model.findColumn("lstg_format_name");

        // non-exist input returns null;
        DictionaryInfo nullInfo = dictMgr.buildDictionary(col, MockupReadableTable.newNonExistTable("/a/path"));
        assertEquals(null, nullInfo);
        
        DictionaryInfo info1 = dictMgr.buildDictionary(col, MockupReadableTable.newSingleColumnTable("/a/path", "1", "2", "3"));
        assertEquals(3, info1.getDictionaryObject().getSize());

        // same input returns same dict
        DictionaryInfo info2 = dictMgr.buildDictionary(col, MockupReadableTable.newSingleColumnTable("/a/path", "1", "2", "3"));
        assertTrue(info1 == info2);
        
        // same input values (different path) returns same dict
        DictionaryInfo info3 = dictMgr.buildDictionary(col, MockupReadableTable.newSingleColumnTable("/a/different/path", "1", "2", "3"));
        assertTrue(info1 == info3);
        
        // save dictionary works in spite of non-exist table
        Dictionary<String> dict = DictionaryGenerator.buildDictionary(col.getType(), new IterableDictionaryValueEnumerator("1", "2", "3"));
        DictionaryInfo info4 = dictMgr.saveDictionary(col, MockupReadableTable.newNonExistTable("/a/path"), dict);
        assertTrue(info1 == info4);
        
        Dictionary<String> dict2 = DictionaryGenerator.buildDictionary(col.getType(), new IterableDictionaryValueEnumerator("1", "2", "3", "4"));
        DictionaryInfo info5 = dictMgr.saveDictionary(col, MockupReadableTable.newNonExistTable("/a/path"), dict2);
        assertTrue(info1 != info5);
    }

    @Test
    public void testDictionaryCache() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        DictionaryManager dictMgr = DictionaryManager.getInstance(config);
        DataModelManager metaMgr = DataModelManager.getInstance(config);
        DataModelDesc model = metaMgr.getDataModelDesc("test_kylin_inner_join_model_desc");
        TblColRef col = model.findColumn("lstg_format_name");

        DictionaryInfo dictionaryInfo = dictMgr.buildDictionary(col, MockupReadableTable.newSingleColumnTable("/a/path", "1", "2", "3"));

        String sourcePath = dictionaryInfo.getResourcePath();
        dictionaryInfo = null;

        Dictionary dictionary = dictMgr.getDictionary(sourcePath);
        assertNotNull(dictionary);

        // 1. clean cache
        releaseDictionary(dictMgr, sourcePath);

        Dictionary dictionary2 = getDictObjByWeakCache(sourcePath, dictMgr);
        assertNotNull(dictionary2);
        assertEquals(dictionary, dictionary2);

        DictionaryInfo dictionaryInfo2 = getDictInfoByWeakCache(sourcePath, dictMgr);
        assertNotNull(dictionaryInfo2);
        dictionaryInfo2 = null;

        // 2. gc
        System.gc();

        Dictionary dictionary3 = getDictObjByWeakCache(sourcePath, dictMgr);
        assertNotNull(dictionary3);
        assertEquals(dictionary, dictionary3);

        DictionaryInfo dictionaryInfo3 = getDictInfoByWeakCache(sourcePath, dictMgr);
        assertNull(dictionaryInfo3);

        Dictionary dictionary4 = dictMgr.getDictionary(sourcePath);
        assertNotNull(dictionary4);
        assertEquals(dictionary, dictionary4);

        // 3. clean cache && clean handle && gc
        releaseDictionary(dictMgr, sourcePath);
        dictionary = null;
        dictionary2 = null;
        dictionary3 = null;
        dictionary4 = null;

        System.gc();

        Dictionary dictionary5 = getDictObjByWeakCache(sourcePath, dictMgr);
        assertNull(dictionary5);

        DictionaryInfo dictionaryInfo5 = getDictInfoByWeakCache(sourcePath, dictMgr);
        assertNull(dictionaryInfo5);

        Dictionary dictionary6 = dictMgr.getDictionary(sourcePath);
        assertNotNull(dictionary6);

    }

    private void releaseDictionary(DictionaryManager dictMgr, String sourcePath) throws NoSuchFieldException, IllegalAccessException {
        getDictCache(dictMgr).invalidate(sourcePath);
    }

    private LoadingCache<String, DictionaryInfo> getDictCache(DictionaryManager dictMgr) throws NoSuchFieldException, IllegalAccessException {
        Class<?> clazz = dictMgr.getClass();
        Field field = clazz.getDeclaredField("dictCache");
        field.setAccessible(true);
        return (LoadingCache<String, DictionaryInfo>) field.get(dictMgr);
    }

    private DictionaryInfo getDictInfoByWeakCache(String resourcePath, DictionaryManager dictMgr) throws Exception {
        Map<String, WeakReference<Object>> weakCache = getWeakCache(dictMgr);

        WeakReference reference = weakCache.get(resourcePath);
        if (reference == null) {
            return null;
        }
        return (DictionaryInfo) reference.get();
    }

    private Map<String, WeakReference<Object>> getWeakCache(DictionaryManager dictMgr) throws NoSuchFieldException, IllegalAccessException {
        Class<?> clazz = dictMgr.getClass();
        Field field = clazz.getDeclaredField("weakDictCache");
        field.setAccessible(true);
        return (Map<String, WeakReference<Object>>) field.get(dictMgr);
    }

    private Dictionary getDictObjByWeakCache(String resourcePath, DictionaryManager dictMgr) throws Exception {
        Map<String, WeakReference<Object>> weakCache = getWeakCache(dictMgr);

        WeakReference reference = weakCache.get(dictMgr.dictObjKey(resourcePath));
        if (reference == null) {
            return null;
        }

        return (Dictionary) reference.get();
    }
}
