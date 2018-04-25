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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
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
import com.google.common.collect.Lists;

public class ProjectDictionaryManager {
    public static final Logger logger = LoggerFactory.getLogger(ProjectDictionaryManager.class);

    private ResourceLock resourceLock = new ResourceLock();
    private final static Cache<String, ProjectDictionaryInfo> dictionaryInfoCache = CacheBuilder.newBuilder()
            .maximumSize(50).expireAfterWrite(1, TimeUnit.HOURS).removalListener(//
                    new RemovalListener<String, ProjectDictionaryInfo>() {
                        @Override
                        public void onRemoval(RemovalNotification<String, ProjectDictionaryInfo> notification) {
                            logger.info("DictionaryInfoCache entry with key {} is removed due to {} ",
                                    notification.getKey(), notification.getCause());
                        }
                    })
            .build();

    private final static Cache<String, DictPatch> patchCache = CacheBuilder.newBuilder().maximumSize(100)
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
    private LoadingCache<String, MultiVersionControl> mvcMap = CacheBuilder.newBuilder()//
            .softValues()//
            .removalListener(new RemovalListener<String, MultiVersionControl>() {
                @Override
                public void onRemoval(RemovalNotification<String, MultiVersionControl> notification) {
                    logger.info("Dict with resource path " + notification.getKey() + " is removed due to "
                            + notification.getCause());
                }
            })//
            .maximumSize(1000)//
            .expireAfterWrite(1, TimeUnit.DAYS).build(new CacheLoader<String, MultiVersionControl>() {
                @Override
                public MultiVersionControl load(String key) throws IOException, InterruptedException {
                    return new MultiVersionControl(key, resourceLock);
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
                ResourceStore.PROJECT_DICT_RESOURCE_ROOT + "/metadata", MetadataConstants.TYPE_VERSION,
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
        long maxVersion = getMaxVersion(segProjectDict).getMaxVersion();
        return getSpecificPatch(segProjectDict, maxVersion);
    }

    public DictPatch getSpecificPatch(SegProjectDict segProjectDict, long toVersion) throws IOException {
        ArrayList<String> patchPaths = getPatchResourceIdentifier(segProjectDict, toVersion);
        switch (patchPaths.size()) {
        case 0:
            return null;
        case 1: // Has segment patch
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
            throw new RuntimeException(
                    "Error for get dictionary patch : " + segProjectDict.getSourceIdentifier());
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
        long toVersion = getMaxVersion(segProjectDict).getMaxVersion();
        paths.add(PathBuilder.verisionPath(segProjectDict.getSourceIdentifier()));
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
        ProjectDictionaryInfo dictionaryInfoIfPresent = dictionaryInfoCache.getIfPresent(dictResPath);
        if (dictionaryInfoIfPresent != null) {
            return dictionaryInfoIfPresent;
        } else {
            try {
                ProjectDictionaryInfo projectDictionaryInfo = doLoad(dictResPath, true);
                // merge step may be null
                if (projectDictionaryInfo != null) {
                    dictionaryInfoCache.put(dictResPath, projectDictionaryInfo);
                }
                return projectDictionaryInfo;
            } catch (IOException e) {
                throw new RuntimeException("Load dictionary " + dictResPath + " failed!", e);
            }
        }
    }

    public ProjectDictionaryInfo getSpecificDictionary(SegProjectDict desc, long SpecificVersion)
            throws IOException {
        if (desc == null) {
            return null;
        }
        Preconditions.checkArgument(desc.getCurrentVersion() <= SpecificVersion);
        ProjectDictionaryInfo projectDictionaryInfo = loadDictByVersion(desc.getSourceIdentifier(), SpecificVersion);
        DictPatch patch = getSpecificPatch(desc, SpecificVersion);
        logger.info("Get Specific Dictionary: " + desc.getSourceIdentifier() + " version : " + SpecificVersion);

        return ProjectDictionaryInfo.copy(projectDictionaryInfo,
                new DisguiseTrieDictionary<>(desc.getIdLength(), projectDictionaryInfo.getDictionaryObject(), patch));
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
            // build step the version is null.
            return null;
        }
        long maxVersion = versionInfo.getMaxVersion();
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

    private ProjectDictionaryInfo doLoad(String resourcePath, boolean loadDictObj) throws IOException {
        ResourceStore store = getStore();
        logger.info("DictionaryManager(" + System.identityHashCode(this) + ") loading DictionaryInfo(loadDictObj:"
                + loadDictObj + ") at " + resourcePath);
        return store.getResource(resourcePath, ProjectDictionaryInfo.class,
                loadDictObj ? ProjectDictionaryInfoSerializer.FULL_SERIALIZER
                        : ProjectDictionaryInfoSerializer.INFO_SERIALIZER);
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(KylinConfig.getInstanceFromEnv());
    }

    // todo save all dict to hdfs too
    public SegProjectDict append(String project, DictionaryInfo dictionaryInfo)
            throws ExecutionException, IOException {
        String sourceIdentify = PathBuilder.sourceIdentifier(project, dictionaryInfo);
        MultiVersionControl mvc = mvcMap.get(sourceIdentify);
        long currentVersion = mvc.getCurrentVersion();
        // check contains
        if (currentVersion > -1) {
            // fetch current dict
            ProjectDictionaryInfo projectDictionaryInfo = loadDictByVersion(sourceIdentify, currentVersion);
            if (projectDictionaryInfo.getDictionaryObject().contains(dictionaryInfo.getDictionaryObject())) {
                logger.info("Dictionary " + sourceIdentify + "be contained version"
                        + projectDictionaryInfo.getDictionaryVersion());
                return new SegProjectDict(sourceIdentify, currentVersion,
                        genSegmentDictionaryToProjectDictionaryMapping(dictionaryInfo, sourceIdentify,
                                projectDictionaryInfo, currentVersion),
                        dictionaryInfo.getDictionaryObject().getSizeOfId());
            }
        }
        MultiVersionControl.VersionEntry versionEntry = mvc.beginAppendWhenPreviousAppendCompleted();
        if (versionEntry.getVersion() == 0) {
            logger.info("Create project dictionary : " + sourceIdentify);
            return createDictionary(sourceIdentify, dictionaryInfo, mvc, versionEntry);
        } else {
            logger.info("Append a new project dictionary with version : " + versionEntry.getVersion());
            return appendDictionary(sourceIdentify, dictionaryInfo, mvc, versionEntry);
        }
    }

    private SegProjectDict appendDictionary(String sourceIdentify, DictionaryInfo dictionaryInfo,
            MultiVersionControl mvc, MultiVersionControl.VersionEntry versionEntry) throws IOException {
        checkInterrupted(sourceIdentify);

        logger.info("Append project dictionary with column: " + sourceIdentify);
        String s = eatAndUpgradeDictionary(dictionaryInfo, sourceIdentify, versionEntry.getVersion(), versionEntry);
        mvc.commit();
        return new SegProjectDict(sourceIdentify, versionEntry.getVersion(), s,
                dictionaryInfo.getDictionaryObject().getSizeOfId());
    }

    private synchronized void versionCheckPoint(String sourceIdentify, MultiVersionControl.VersionEntry versionEntry,
            ProjectDictionaryVersionInfo projectDictionaryVersion, int sizeOfId) throws IOException {
        if (projectDictionaryVersion != null) {
            ProjectDictionaryVersionInfo copy = projectDictionaryVersion.copy();
            copy.setMaxVersion(versionEntry.getVersion());
            crud.save(copy);
        } else {
            crud.save(new ProjectDictionaryVersionInfo(sourceIdentify, versionEntry.getVersion(), sizeOfId));
        }
    }

    private SegProjectDict createDictionary(String sourceIdentify, DictionaryInfo dictionaryInfo,
            MultiVersionControl mvc, MultiVersionControl.VersionEntry versionEntry) throws IOException {
        checkInterrupted(sourceIdentify);

        logger.info("create project dictionary : " + sourceIdentify);
        ProjectDictionaryInfo warp = ProjectDictionaryInfo.wrap(dictionaryInfo, versionEntry.getVersion());
        saveDictionary(sourceIdentify, warp);
        mvc.commit();
        ProjectDictionaryVersionInfo versionInfo = versionCache.get(PathBuilder.versionKey(sourceIdentify));
        versionCheckPoint(sourceIdentify, versionEntry, versionInfo,
                dictionaryInfo.getDictionaryObject().getSizeOfId());
        return new SegProjectDict(sourceIdentify, versionEntry.getVersion(),
                dictionaryInfo.getDictionaryObject().getSizeOfId());
    }

    protected void saveDictionary(String sourceIdentify, ProjectDictionaryInfo dictionaryInfo) throws IOException {
        checkInterrupted(sourceIdentify);
        String path = PathBuilder.dataPath(sourceIdentify, dictionaryInfo.getDictionaryVersion());
        logger.info("Saving dictionary at " + path + "version is :" + dictionaryInfo.getDictionaryObject());
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buf);
        ProjectDictionaryInfoSerializer.FULL_SERIALIZER.serialize(dictionaryInfo, out);
        out.close();
        buf.close();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(buf.toByteArray());
        getStore().putResource(path, inputStream, System.currentTimeMillis());
        Path sDictDir = new Path(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/sparder/sdict");
        FileSystem workingFileSystem = HadoopUtil.getWorkingFileSystem();
        if (!workingFileSystem.exists(sDictDir)) {
            workingFileSystem.mkdirs(sDictDir);
        }

        Path f = new Path(sDictDir,
                new Path(sourceIdentify + "/" + dictionaryInfo.getDictionaryVersion() + "/data.sdict"));
        try (FSDataOutputStream out1 = workingFileSystem.create(f)) {
            SDict.wrap(dictionaryInfo.getDictionaryObject()).write(out1);
        }
    }

    private String eatAndUpgradeDictionary(DictionaryInfo originDict, String sourceIdentify, long version,
            MultiVersionControl.VersionEntry versionEntry) throws IOException {
        ProjectDictionaryInfo beforeVersion = loadDictByVersion(sourceIdentify, version - 1);
        DictionaryInfo mergedDictionary = dictionaryManager
                .mergeDictionary(Lists.newArrayList(beforeVersion, originDict));
        ProjectDictionaryInfo warp = ProjectDictionaryInfo.wrap(mergedDictionary, version);
        // patch 1
        saveDictionary(sourceIdentify, warp);
        for (int i = 0; i < version; i++) {
            ProjectDictionaryInfo historyVersion = loadDictByVersion(sourceIdentify, i);
            int[] value = ProjectDictionaryHelper.genOffset(historyVersion, mergedDictionary);
            saveMapping(sourceIdentify, i, version, value);
        }
        ProjectDictionaryVersionInfo versionInfo = versionCache.get(PathBuilder.versionKey(sourceIdentify));
        versionCheckPoint(sourceIdentify, versionEntry, versionInfo,
                mergedDictionary.getDictionaryObject().getSizeOfId());

        if (originDict.getDictionaryObject().getSize() > 0) {
            return genSegmentDictionaryToProjectDictionaryMapping(originDict, sourceIdentify, mergedDictionary,
                    version);
        } else {
            return null;
        }
    }

    private String genSegmentDictionaryToProjectDictionaryMapping(DictionaryInfo originDict, String sourceIdentify,
            DictionaryInfo mergedDictionary, long version) throws IOException {
        int[] offset = ProjectDictionaryHelper.genOffset(originDict, mergedDictionary);
        // different version has different patch
        String path = PathBuilder.segmentPatchPath(sourceIdentify, originDict.getUuid(), version);
        saveMappingToPath(offset, path);
        return path;
    }

    private void saveMapping(String sourceIdentify, long currentVersion, long toVersion, int[] value)
            throws IOException {
        checkInterrupted(sourceIdentify);
        String path = PathBuilder.patchPath(sourceIdentify, currentVersion, toVersion);
        ResourceStore store = getStore();
        store.putResource(path, new DictPatch(value), DICT_PATCH_SERIALIZER);
    }

    private void saveMappingToPath(int[] value, String path) throws IOException {
        ResourceStore store = getStore();
        if (!store.exists(path)) {
            store.putResource(path, new DictPatch(value), DICT_PATCH_SERIALIZER);
        }
    }

    //  remove mvc, to be init again
    public void shutdown() {
        logger.info("Shut down project dictionary manager.");
        for (Map.Entry<String, MultiVersionControl> mvc : mvcMap.asMap().entrySet()) {
            mvc.getValue().clear();
            this.mvcMap.invalidate(mvc.getKey());
        }
    }

    //  remove mvc, to be init again
    public void init() {
        logger.info("Init project dictionary manager.");
        mvcMap.asMap().clear();
    }

    protected void checkInterrupted(String sourceIdentify) {
        if (Thread.interrupted()) {
            try {
                MultiVersionControl multiVersionControl = mvcMap.get(sourceIdentify);
                multiVersionControl.commit();
            } catch (ExecutionException e) {
                logger.info("Failed to find mvc  with column :" + sourceIdentify);
            }
            throw new RuntimeException(new InterruptedException("Interrupted build dictionary : " + sourceIdentify));
        }
    }
}
