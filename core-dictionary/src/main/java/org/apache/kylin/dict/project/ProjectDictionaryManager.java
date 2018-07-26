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

import static org.apache.kylin.dict.project.DictPatch.DictPatchSerializer.DICT_PATCH_SERIALIZER;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.WriteConflictException;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.SDict;
import org.apache.kylin.dict.project.ProjectDictionaryHelper.PathBuilder;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ProjectDictionaryManager {
    public static final Logger logger = LoggerFactory.getLogger(ProjectDictionaryManager.class);
    private static ConcurrentMap<String, Long> dictCacheSwapCount = Maps.newConcurrentMap();
    private static final ProjectDictionaryInfo NONE_INDICATOR = new ProjectDictionaryInfo();
    private static AtomicLong usedMem = new AtomicLong(0);
    private final static Map<String, WeakReference<Object>> weakDictCache = new ConcurrentHashMap<>();
    private final static LoadingCache<String, ProjectDictionaryInfo> dictionaryInfoCache = CacheBuilder.newBuilder()
            .maximumWeight(KylinConfig.getInstanceFromEnv().getCachedPrjDictMaxMemory())
            .weigher(new Weigher<String, ProjectDictionaryInfo>() {
                @Override
                public int weigh(String sourceIdentity, ProjectDictionaryInfo dict) {
                    double estimateBytes = dict.getByteSize() * 1.5;
                    long estimateMB = (long) (estimateBytes / (1024 * 1024));
                    logger.info("Add project dictionary to cache, size : " + estimateMB + " M. Current used memory: "
                            + usedMem + " M");
                    if (dictCacheSwapCount.containsKey(sourceIdentity)) {
                        dictCacheSwapCount.put(sourceIdentity, dictCacheSwapCount.get(sourceIdentity) + 1);
                    } else {
                        dictCacheSwapCount.put(sourceIdentity, 1L);
                    }
                    Long aLong = dictCacheSwapCount.get(sourceIdentity);
                    logger.info("Load project dictionary " + sourceIdentity + " " + aLong + " times");
                    usedMem.addAndGet(estimateMB);
                    return (int) estimateMB;
                }
            }).expireAfterWrite(1, TimeUnit.HOURS).removalListener(//
                    new RemovalListener<String, ProjectDictionaryInfo>() {
                        @Override
                        public void onRemoval(RemovalNotification<String, ProjectDictionaryInfo> notification) {
                            if (notification.getValue() != null) {
                                long estimateBytes = notification.getValue().getByteSize();
                                long estimateMB = estimateBytes / (1024 * 1024);
                                usedMem.addAndGet(-estimateMB);
                                logger.info("DictionaryInfoCache entry with key {} is removed due to {} ",
                                        notification.getKey(), notification.getCause() + ", release size : "
                                                + estimateMB + " M," + " Current use memory: " + usedMem);
                            } else {
                                logger.info("DictionaryInfoCache with resource path " + notification.getKey()
                                        + " is removed due to " + notification.getCause());
                            }
                        }
                    })
            .build(new CacheLoader<String, ProjectDictionaryInfo>() {
                @Override
                public ProjectDictionaryInfo load(String sourceIdentity) throws Exception {
                    logger.info("Loading project dictionary :" + sourceIdentity);
                    ProjectDictionaryInfo projectDictionaryInfo = null;
                    Dictionary dictObj = null;
                    WeakReference reference = weakDictCache.get(sourceIdentity);
                    if (reference != null) {
                        projectDictionaryInfo = (ProjectDictionaryInfo) reference.get();
                    }
                    if (null != projectDictionaryInfo) {
                        logger.info("get dictInfo from weakDictCache success");
                        return projectDictionaryInfo;
                    } else {
                        // if the dictInfo's reference has cleaned, but the dictObj reference is handled by others,
                        // just load the dictInfo without dictObj, and fill it with the existing dictObj
                        reference = weakDictCache.get(dictObjKey(sourceIdentity));
                        if (reference != null) {
                            dictObj = (Dictionary) reference.get();
                        }
                        if (dictObj != null) {
                            logger.info("get dictObj from weakDictCache success");
                            projectDictionaryInfo = doLoad(sourceIdentity, false);
                            if (projectDictionaryInfo != null) {
                                projectDictionaryInfo.setDictionaryObject(dictObj);
                            }
                        } else {
                            projectDictionaryInfo = doLoad(sourceIdentity, true);
                        }
                    }
                    if (projectDictionaryInfo == null) {
                        return NONE_INDICATOR;
                    } else {
                        weakDictCache.put(sourceIdentity, new WeakReference<>(projectDictionaryInfo));
                        weakDictCache.put(dictObjKey(sourceIdentity),
                                new WeakReference<>(projectDictionaryInfo.getDictionaryObject()));
                        logger.info("weakDictCache size : " + weakDictCache.size());
                        return projectDictionaryInfo;
                    }
                }
            });

    public static String dictObjKey(String sourceIdentity) {
        return sourceIdentity + "_obj";
    }

    private final static Cache<String, DictPatch> patchCache = CacheBuilder.newBuilder().softValues()
            .maximumSize(KylinConfig.getInstanceFromEnv().getCachedPrjDictPatchMaxEntrySize())
            .expireAfterWrite(1, TimeUnit.HOURS).removalListener(//
                    new RemovalListener<String, DictPatch>() {
                        @Override
                        public void onRemoval(RemovalNotification<String, DictPatch> notification) {
                            logger.info("PatchCache entry with key {} is removed due to {} ", notification.getKey(),
                                    notification.getCause());
                        }
                    })
            .build();

    private CaseInsensitiveStringCache<ProjectDictionaryVersionInfo> versionCache;
    private CachedCrudAssist<ProjectDictionaryVersionInfo> crud;
    // only use by  job node
    private final static LoadingCache<String, VersionControl> mvcMap = CacheBuilder.newBuilder()
            .maximumSize(Long.MAX_VALUE).removalListener(new RemovalListener<String, VersionControl>() {
                @Override
                public void onRemoval(RemovalNotification<String, VersionControl> notification) {
                    logger.info("Dict with resource path " + notification.getKey() + " is removed due to "
                            + notification.getCause());
                    if (notification.getValue() != null)
                        notification.getValue().clear();
                }
            }).expireAfterWrite(1, TimeUnit.DAYS).build(new CacheLoader<String, VersionControl>() {
                @Override
                public VersionControl load(String key) throws IOException, InterruptedException {
                    return new VersionControl(key);
                }
            });

    private volatile static ProjectDictionaryManager ProjectDictionaryManager;
    private DictionaryManager dictionaryManager;

    // only for test to change private to public
    public ProjectDictionaryManager() throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        this.dictionaryManager = DictionaryManager.getInstance(kylinConfig);
        this.versionCache = new CaseInsensitiveStringCache<>(kylinConfig, "project_dictionary_version");
        this.crud = new CachedCrudAssist<ProjectDictionaryVersionInfo>(getStore(),
                ResourceStore.PROJECT_DICT_RESOURCE_ROOT + "/version_info", MetadataConstants.TYPE_VERSION,
                ProjectDictionaryVersionInfo.class, versionCache, false) {
            @Override
            protected ProjectDictionaryVersionInfo initEntityAfterReload(
                    ProjectDictionaryVersionInfo projectDictionaryVersionInfo, String resourceName) {
                return projectDictionaryVersionInfo;
            }
        };
        this.crud.setCheckCopyOnWrite(true);

        // touch lower level metadata before registering my listener
        crud.reloadAll();
        Broadcaster.getInstance(kylinConfig).registerListener(new ProjectDictionarySyncListener(),
                "project_dictionary_version");
    }

    private class ProjectDictionarySyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            crud.reload(cacheKey);
        }
    }

    public static ProjectDictionaryManager getInstance() {
        if (ProjectDictionaryManager == null) {
            synchronized (ProjectDictionaryManager.class) {
                if (ProjectDictionaryManager == null) {
                    try {
                        ProjectDictionaryManager = new ProjectDictionaryManager();
                    } catch (IOException e) {
                        e.printStackTrace();
                        logger.error(e.getMessage());
                    }
                }
            }
        }
        return ProjectDictionaryManager;
    }

    public ProjectDictionaryVersionInfo getMaxVersion(SegProjectDict desc) {
        return getMaxVersion(desc.getSourceIdentifier());
    }

    public ProjectDictionaryVersionInfo getMaxVersion(String sourceIdentify) {
        String s = PathBuilder.versionKey(sourceIdentify);
        return versionCache.get(s);
    }

    public DictPatch getMaxVersionPatch(SegProjectDict segProjectDict) throws IOException {
        long maxVersion = getMaxVersion(segProjectDict).getProjectDictionaryVersion();
        return getSpecificPatch(segProjectDict, maxVersion);
    }

    public DictPatch getSpecificPatch(SegProjectDict segProjectDict, long toVersion) throws IOException {
        ArrayList<String> patchPaths = getPatchResourceIdentifier(segProjectDict, toVersion);
        switch (patchPaths.size()) {
        case 0: // Dict is project dictionary
            return null;
        case 1: // Only has segment patch or only has prj dict to prj dict patch
            return loadDictPatch(patchPaths.get(0));
        case 2:
            String cacheKey = String.join(",", patchPaths);
            DictPatch mergedPatch = patchCache.getIfPresent(cacheKey);
            if (mergedPatch != null) {
                return mergedPatch;
            }
            // 0 is segment patch, 1 is prj dict to prj dict patch
            mergedPatch = loadDictPatch(patchPaths.get(0)).upgrade(loadDictPatch(patchPaths.get(1)));
            patchCache.put(cacheKey, mergedPatch);
            return mergedPatch;
        default:
            throw new RuntimeException("Error for get dictionary patch : " + segProjectDict.getSourceIdentifier());
        }
    }

    private ArrayList<String> getPatchResourceIdentifier(SegProjectDict segProjectDict, long toVersion) {
        Preconditions.checkState(segProjectDict.getCurrentVersion() <= toVersion);
        logger.info("Get patch for : " + segProjectDict.getSourceIdentifier());
        ArrayList<String> patchPaths = Lists.newArrayList();
        if (segProjectDict.getSegPatch() != null) {
            logger.info("Add segment patch path: " + segProjectDict.getSegPatch());
            patchPaths.add(segProjectDict.getSegPatch());
        }
        // need upgrade
        if (segProjectDict.getCurrentVersion() != toVersion) {
            String projectToProjectDictPatch = PathBuilder.patchPath(segProjectDict.getSourceIdentifier(),
                    segProjectDict.getCurrentVersion(), toVersion);
            logger.info("Add project dict to project dict patch path: " + projectToProjectDictPatch);
            patchPaths.add(projectToProjectDictPatch);
        }
        return patchPaths;
    }

    /**
     * It's use for merge cube -> MergeDictionaryStep
     *
     * @param segProjectDict
     * @return Resource path
     * @throws IOException
     */
    public List<String> getPatchMetaStore(SegProjectDict segProjectDict) throws IOException {
        List<String> paths = Lists.newArrayList();
        long toVersion = getMaxVersion(segProjectDict).getProjectDictionaryVersion();
        paths.add(PathBuilder.versionPath(segProjectDict.getSourceIdentifier()));
        paths.add(PathBuilder.dataPath(segProjectDict.getSourceIdentifier(), toVersion));
        paths.addAll(getPatchResourceIdentifier(segProjectDict, toVersion));
        logger.info("Get metadata with : " + segProjectDict + ", version: " + toVersion + ", paths: "
                + String.join(",", paths));
        return paths;
    }

    private DictPatch loadDictPatch(String path) throws IOException {
        DictPatch dictPatch = patchCache.getIfPresent(path);
        if (dictPatch != null) {
            logger.info("Hint patch cache : " + path);
            return dictPatch;
        }
        logger.info("Load project dictionary patch: " + path);
        ResourceStore store = getStore();
        DictPatch resource = store.getResource(path, DictPatch.class, DICT_PATCH_SERIALIZER);
        // dict  != null  ;  never
        if (resource == null) {
            throw new RuntimeException("Error for get dictionary patch.");
        }
        patchCache.put(path, resource);
        return resource;
    }

    public ProjectDictionaryInfo getDictionary(String dictResPath) {
        logger.info("Get project dictionary : " + dictResPath);
        ProjectDictionaryInfo result = null;
        try {
            result = dictionaryInfoCache.get(dictResPath);
            if (result == NONE_INDICATOR) {
                return null;
            } else {
                return result;
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }

    }

    public ProjectDictionaryInfo getSpecificDictionary(SegProjectDict desc, long SpecificVersion) throws IOException {
        if (desc == null) {
            return null;
        }
        Preconditions.checkArgument(desc.getCurrentVersion() <= SpecificVersion);
        ProjectDictionaryInfo projectDictionaryInfo = loadDictByVersion(desc.getSourceIdentifier(), SpecificVersion);
        DictPatch patch = getSpecificPatch(desc, SpecificVersion);
        logger.info("Get Specific Dictionary: " + desc.getSourceIdentifier() + " version : " + SpecificVersion);

        return projectDictionaryInfo == null ? null
                : ProjectDictionaryInfo.copy(projectDictionaryInfo, new DisguiseTrieDictionary<>(desc.getIdLength(),
                        projectDictionaryInfo.getDictionaryObject(), patch));
    }

    public ProjectDictionaryInfo getSpecificDictWithOutPatch(SegProjectDict desc, long SpecificVersion) {
        ProjectDictionaryInfo projectDictionaryInfo = loadDictByVersion(desc.getSourceIdentifier(), SpecificVersion);
        if (projectDictionaryInfo == null) {
            throw new RuntimeException(" error dictionary");
        }
        logger.info("get dictionary: " + desc.getSourceIdentifier());
        return ProjectDictionaryInfo.copy(projectDictionaryInfo,
                new DisguiseTrieDictionary<>(desc.getIdLength(), projectDictionaryInfo.getDictionaryObject(), null));
    }

    /**
     * @param desc the segment project dict desc
     * @return Combinatio dictionary  =  patch + max version dictionary
     * @throws IOException ioe
     */
    public ProjectDictionaryInfo getCombinationDictionary(SegProjectDict desc) throws IOException {
        ProjectDictionaryVersionInfo versionInfo = getMaxVersion(desc);
        if (versionInfo == null) {
            logger.info("Max info is null");
            // build step the version is null.
            return null;
        }
        logger.info("Get max version info : " + versionInfo.getVersion());

        long maxVersion = versionInfo.getProjectDictionaryVersion();
        return getSpecificDictionary(desc, maxVersion);
    }

    /**
     * @param baseDir s
     * @param version
     * @return
     */
    private ProjectDictionaryInfo loadDictByVersion(String baseDir, long version) {
        String dictResPath = PathBuilder.dataPath(baseDir, version);
        return getDictionary(dictResPath);
    }

    private static ProjectDictionaryInfo doLoad(String resourcePath, boolean loadDictObj) throws IOException {
        ResourceStore store = getStore();
        logger.info("Loading Project DictionaryInfo(loadDictObj:" + loadDictObj + ") at " + resourcePath);
        return store.getResource(resourcePath, ProjectDictionaryInfo.class,
                loadDictObj ? ProjectDictionaryInfoSerializer.FULL_SERIALIZER
                        : ProjectDictionaryInfoSerializer.INFO_SERIALIZER);
    }

    private static ResourceStore getStore() {
        return ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
    }

    public SegProjectDict append(String project, DictionaryInfo dictionaryInfo)
            throws ExecutionException, IOException, InterruptedException {
        String sourceIdentify = PathBuilder.sourceIdentifier(project, dictionaryInfo);
        logger.info("Begin append dict : " + sourceIdentify);
        VersionControl mvc;
        synchronized (ProjectDictionaryManager.class) {
            mvc = mvcMap.get(sourceIdentify);
        }
        long currentVersion = mvc.getCurrentVersion();
        logger.info("Current version is : " + currentVersion);
        // check contains
        if (currentVersion > -1) {
            // fetch current dict
            ProjectDictionaryInfo projectDictionaryInfo = loadDictByVersion(sourceIdentify, currentVersion);
            if (projectDictionaryInfo.getDictionaryObject().contains(dictionaryInfo.getDictionaryObject())) {
                logger.info("Dictionary " + sourceIdentify + "be contained version "
                        + projectDictionaryInfo.getDictionaryVersion());
                return new SegProjectDict(sourceIdentify, currentVersion,
                        genSegmentDictionaryToProjectDictionaryPatch(dictionaryInfo, sourceIdentify,
                                projectDictionaryInfo, currentVersion),
                        dictionaryInfo.getDictionaryObject().getSizeOfId());
            }
        }
        long version = mvc.acquireMyVersion();
        int retry = KylinConfig.getInstanceFromEnv().getProjectDictionaryAppendRetryTimes();
        Throwable throwable = null;
        while (retry > 0) {
            try {
                if (version == 0) {
                    logger.info("Create project dictionary : " + sourceIdentify);
                    return createDictionary(project, sourceIdentify, dictionaryInfo, mvc, version);
                } else {
                    logger.info("Append a new project dictionary with version : " + version);
                    return appendDictionary(project, sourceIdentify, dictionaryInfo, mvc, version);
                }
            } catch (Throwable th) {
                if (th instanceof InterruptedException) {
                    mvc.commit(false);
                    throw th;
                }
                throwable = th;
                retry--;
                logger.error("Error for append project dictionary : " + sourceIdentify + ", retry after 10000ms.", th);
                try {
                    Thread.sleep(10000L);
                } catch (InterruptedException e) {
                    logger.error("Retry sleep interrupted.", e);
                    mvc.commit(false);
                    throw new RuntimeException("Error for retry sleep", e);
                }
            }
        }
        mvc.commit(false);
        logger.error("Error for append project dictionary : " + sourceIdentify, throwable);
        throw new RuntimeException(throwable);
    }

    private SegProjectDict appendDictionary(String project, String sourceIdentify, DictionaryInfo dictionaryInfo,
            VersionControl mvc, long versionEntry) throws IOException, InterruptedException {
        checkInterrupted(sourceIdentify);

        logger.info("Append project dictionary with column: " + sourceIdentify);
        String s = eatAndUpgradeDictionary(project, dictionaryInfo, sourceIdentify, versionEntry);
        mvc.commit(true);
        return new SegProjectDict(sourceIdentify, versionEntry, s, dictionaryInfo.getDictionaryObject().getSizeOfId());
    }

    private synchronized void versionCheckPoint(String sourceIdentify, long version,
            ProjectDictionaryVersionInfo projectDictionaryVersion, int sizeOfId)
            throws IOException, InterruptedException {
        checkInterrupted(sourceIdentify);
        if (projectDictionaryVersion != null) {
            ProjectDictionaryVersionInfo copy = projectDictionaryVersion.copy();
            copy.setProjectDictionaryVersion(version);
            crud.save(copy);
        } else {
            crud.save(new ProjectDictionaryVersionInfo(sourceIdentify, version, sizeOfId));
        }
    }

    private SegProjectDict createDictionary(String project, String sourceIdentify, DictionaryInfo dictionaryInfo,
            VersionControl mvc, long version) throws IOException, InterruptedException {
        checkInterrupted(sourceIdentify);
        logger.info("create project dictionary : " + sourceIdentify);
        ProjectDictionaryInfo warp = ProjectDictionaryInfo.wrap(dictionaryInfo, version);
        saveDictionary(project, sourceIdentify, warp);
        mvc.commit(true);
        ProjectDictionaryVersionInfo versionInfo = versionCache.get(PathBuilder.versionKey(sourceIdentify));
        versionCheckPoint(sourceIdentify, version, versionInfo, dictionaryInfo.getDictionaryObject().getSizeOfId());
        return new SegProjectDict(sourceIdentify, version, dictionaryInfo.getDictionaryObject().getSizeOfId());
    }

    protected void saveDictionary(String project, String sourceIdentify, ProjectDictionaryInfo dictionaryInfo)
            throws IOException, InterruptedException {
        checkInterrupted(sourceIdentify);
        String path = PathBuilder.dataPath(sourceIdentify, dictionaryInfo.getDictionaryVersion());
        logger.info("Saving dictionary at " + path + "version is :" + dictionaryInfo.getDictionaryObject());
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buf);
        ProjectDictionaryInfoSerializer.FULL_SERIALIZER.serialize(dictionaryInfo, out);
        out.close();
        buf.close();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(buf.toByteArray());
        if (getStore().exists(path)) {
            logger.warn("Project dictionary is exists : " + sourceIdentify);
        }
        // if a failed segment is followed by another segment, project dict may need overwrite
        getStore().putResource(path, inputStream, System.currentTimeMillis());
        saveSdict(project, sourceIdentify, dictionaryInfo);
        String newKey = PathBuilder.dataPath(sourceIdentify, dictionaryInfo.getDictionaryVersion());
        dictionaryInfoCache.put(newKey, dictionaryInfo);
        weakDictCache.put(newKey, new WeakReference<>(dictionaryInfo));
        weakDictCache.put(dictObjKey(newKey), new WeakReference<>(dictionaryInfo.getDictionaryObject()));
    }

    private void saveSdict(String project, String sourceIdentify, ProjectDictionaryInfo dictionaryInfo)
            throws InterruptedException {
        int retry = KylinConfig.getInstanceFromEnv().getProjectDictionaryAppendRetryTimes();
        Throwable throwable = null;
        while (retry > 0) {
            try {
                KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
                Path sDictDir = new Path(
                        kylinConfig.getReadHdfsWorkingDirectory(project) + PathBuilder.SPARDER_DICT_ROOT);
                FileSystem workingFileSystem = HadoopUtil.getReadFileSystem();
                if (!workingFileSystem.exists(sDictDir)) {
                    workingFileSystem.mkdirs(sDictDir);
                }
                Path f = new Path(sDictDir, new Path(
                        sourceIdentify + "/" + dictionaryInfo.getDictionaryVersion() + PathBuilder.SDICT_DATA));
                try (FSDataOutputStream out1 = workingFileSystem.create(f)) {
                    SDict.wrap(dictionaryInfo.getDictionaryObject()).write(out1);
                }
                return;
            } catch (Throwable th) {
                try {
                    throwable = th;
                    logger.error("Error for save sdict : " + sourceIdentify + ", version :"
                            + dictionaryInfo.getDictionaryVersion() + ", retry after 10000ms.", th);
                    retry--;
                    Thread.sleep(10000L);
                } catch (InterruptedException e) {
                    logger.error("Retry sleep interrupted.", e);
                    throw e;
                }

            }
        }
        throw new RuntimeException(
                "Error for save sdict : " + sourceIdentify + ", version :" + dictionaryInfo.getDictionaryVersion(),
                throwable);
    }

    private String eatAndUpgradeDictionary(String project, DictionaryInfo originDict, String sourceIdentify,
            long toVersion) throws IOException, InterruptedException {
        ProjectDictionaryInfo previousDictionary = loadDictByVersion(sourceIdentify, toVersion - 1);
        DictionaryInfo mergedDictionary = dictionaryManager
                .mergeDictionary(Lists.newArrayList(previousDictionary, originDict));
        ProjectDictionaryInfo warp = ProjectDictionaryInfo.wrap(mergedDictionary, toVersion);
        // patch 1
        saveDictionary(project, sourceIdentify, warp);
        int[] lastPatch = ProjectDictionaryHelper.genOffset(previousDictionary, mergedDictionary);
        long beforeVersion = toVersion - 1;
        for (int currentVersion = 0; currentVersion < toVersion; currentVersion++) {
            if (currentVersion < beforeVersion) {
                //  0-2 patch = 0-1 + 1-2
                String beforePatch = PathBuilder.patchPath(sourceIdentify, currentVersion, beforeVersion);
                DictPatch dictPatch = loadDictPatch(beforePatch);
                int[] patch = dictPatch.genNewOffset(lastPatch);
                savePatch(sourceIdentify, currentVersion, toVersion, patch);
            } else {
                savePatch(sourceIdentify, currentVersion, toVersion, lastPatch);

            }
        }
        ProjectDictionaryVersionInfo versionInfo = versionCache.get(PathBuilder.versionKey(sourceIdentify));
        versionCheckPoint(sourceIdentify, toVersion, versionInfo, mergedDictionary.getDictionaryObject().getSizeOfId());

        if (originDict.getDictionaryObject().getSize() > 0) {
            return genSegmentDictionaryToProjectDictionaryPatch(originDict, sourceIdentify, mergedDictionary,
                    toVersion);
        } else {
            return null;
        }
    }

    private String genSegmentDictionaryToProjectDictionaryPatch(DictionaryInfo originDict, String sourceIdentify,
            DictionaryInfo mergedDictionary, long version) throws IOException, InterruptedException {
        int[] offset = ProjectDictionaryHelper.genOffset(originDict, mergedDictionary);
        // different version has different patch
        String path = PathBuilder.segmentPatchPath(sourceIdentify, originDict.getUuid(), version);
        savePatchToPath(offset, path);
        return path;
    }

    private void savePatch(String sourceIdentify, long currentVersion, long toVersion, int[] value)
            throws IOException, InterruptedException {
        checkInterrupted(sourceIdentify);
        String path = PathBuilder.patchPath(sourceIdentify, currentVersion, toVersion);
        ResourceStore store = getStore();
        if (store.listResources(path) == null) {
            logger.info("Saving patch at " + sourceIdentify + "version is " + currentVersion + " to " + toVersion);
            patchCache.put(path, new DictPatch(value));

            savePatch(value, path, store);
        } else {
            logger.info("Patch is exist, skip save patch : " + path);
        }
    }

    private void savePatchToPath(int[] value, String path) throws IOException, InterruptedException {
        ResourceStore store = getStore();
        if (store.listResources(path) == null) {
            logger.info("Saving patch at " + path);
            patchCache.put(path, new DictPatch(value));
            savePatch(value, path, store);
        } else {
            logger.info("Patch is exist, skip save patch : " + path);
        }
    }

    private void savePatch(int[] value, String path, ResourceStore store) throws IOException, InterruptedException {
        long retry = 100;
        Throwable th = null;
        while (retry > 0) {
            try {
                store.putResource(path, new DictPatch(value), DICT_PATCH_SERIALIZER);
                return;
            } catch (WriteConflictException e) {
                th = e;
                logger.info("Find WriteConflictException, Sleep 100 ms.", e);
                Thread.sleep(100L);
                retry--;
                if (store.getResource(path, DictPatch.class, DICT_PATCH_SERIALIZER) != null) {
                    logger.info("Patch is exist, skip save patch : " + path);
                    return;
                }
            }
        }
        throw new RuntimeException("Error for save patch: " + path, th);
    }

    //  remove mvc, to be init again
    public void shutdown() {
        logger.info("Shut down project dictionary manager.");
        for (Map.Entry<String, VersionControl> mvc : mvcMap.asMap().entrySet()) {
            mvc.getValue().clear();
            mvcMap.invalidate(mvc.getKey());
        }
    }

    // only for Migration
    public void clear() {
        shutdown();
        try {
            crud.reloadAll();
            dictionaryInfoCache.invalidateAll();
            patchCache.invalidateAll();
            weakDictCache.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //  remove mvc, to be init again
    public void init() {
        logger.info("Init project dictionary manager.");
        mvcMap.asMap().clear();
    }

    protected void checkInterrupted(String sourceIdentify) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException("Interrupted build dictionary : " + sourceIdentify);
        }
    }

    public void deleteAllResource() throws IOException {
        // delete dict
        NavigableSet<String> tobeDelete = getStore().listResourcesRecursively(ResourceStore.PROJECT_DICT_RESOURCE_ROOT);
        if (tobeDelete != null) {
            for (String deletePath : tobeDelete) {
                getStore().deleteResource(deletePath);
            }
        }
    }
}
