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
import static org.apache.kylin.dict.project.ProjectDictionaryHelper.PathBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ProjectDictionaryManager {
    public static final Logger logger = LoggerFactory.getLogger(ProjectDictionaryManager.class);

    // todo: use cache
    private Map<String, ProjectDictionaryInfo> dictionaryInfoMap = Maps.newConcurrentMap();
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
                public MultiVersionControl load(String key) {
                    return new MultiVersionControl(key);
                }
            });

    private volatile static ProjectDictionaryManager ProjectDictionaryManager;
    private DictionaryManager dictionaryManager;

    private class CubeSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            crud.reload(cacheKey);
        }
    }

    private ProjectDictionaryManager() throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        this.dictionaryManager = DictionaryManager.getInstance(kylinConfig);
        this.versionCache = new CaseInsensitiveStringCache<>(kylinConfig, "project_dictionary_version");
        this.crud = new CachedCrudAssist<ProjectDictionaryVersionInfo>(getStore(),
                ResourceStore.PROJECT_DICT_RESOURCE_ROOT + "/metadata", MetadataConstants.TYPE_VERSION,
                ProjectDictionaryVersionInfo.class, versionCache, false) {
            @Override
            protected ProjectDictionaryVersionInfo initEntityAfterReload(ProjectDictionaryVersionInfo projectDictionaryVersionInfo,
                    String resourceName) {
                //                versionCache.put(mvdVersion.getSourceIdentify(), mvdVersion);
                return projectDictionaryVersionInfo;
            }
        };
        this.crud.setCheckCopyOnWrite(true);

        // touch lower level metadata before registering my listener
        crud.reloadAll();
                Broadcaster.getInstance(kylinConfig).registerListener(new ProjectDictionarySyncListener(), "mvd_version");
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

    public ProjectDictionaryVersionInfo getMaxVersion(SegmentProjectDictDesc desc) {
        return getMaxVersion(desc.getSourceIdentify());
    }

    public ProjectDictionaryVersionInfo getMaxVersion(String sourceIdentify) {
        String s = PathBuilder.versionKey(sourceIdentify);
        return versionCache.get(s);
    }

    public DictPatch getMaxVersionPatch(SegmentProjectDictDesc segmentProjectDictDesc) throws IOException {
        long maxVersion = getMaxVersion(segmentProjectDictDesc).getMaxVersion();
        return getSpecialPatch(segmentProjectDictDesc, maxVersion);
    }

    // todo :
    public DictPatch getSpecialPatch(SegmentProjectDictDesc segmentProjectDictDesc, long toVersion) throws IOException {
        logger.info("Get patch for : " + segmentProjectDictDesc.getSourceIdentify());
        String patchPath = PathBuilder.patchPath(segmentProjectDictDesc.getSourceIdentify(),
                segmentProjectDictDesc.getCurrentVersion(), toVersion);
        DictPatch patch = null;
        // need upgrade
        if (segmentProjectDictDesc.getCurrentVersion() != toVersion) {
            patch = loadDictPatch(patchPath);
        }
        //  current version is max version
        if (patch == null && segmentProjectDictDesc.getPatch() == null) {
            logger.info("The dictionary is current segment dictionary, skip all patch.");
            return null;
        }
        //  is first version
        if (segmentProjectDictDesc.getPatch() == null) {
            logger.info("The segment dictionary is first version, so it's hasn't segment patch.");
            return patch;
        }
        //  is max version
        if (patch == null && segmentProjectDictDesc.getPatch() != null) {
            logger.info("The segment dictionary is max version project dictionary ,return segment dictionary");
            return loadDictPatch(segmentProjectDictDesc.getPatch());
        }

        DictPatch origin = loadDictPatch(segmentProjectDictDesc.getPatch());
        return origin.upgrade(patch);
    }

    /**
     *  It's use for merge cube -> MergeDictionaryStep
     * @param segmentProjectDictDesc
     * @return Resource path
     * @throws IOException
     */
    public List<String> getPatchMetaStore(SegmentProjectDictDesc segmentProjectDictDesc) throws IOException {
        List<String> paths = Lists.newArrayList();
        long toVersion = getMaxVersion(segmentProjectDictDesc).getMaxVersion();
        paths.add(PathBuilder.verisionPath(segmentProjectDictDesc.getSourceIdentify()));
        //        if (!toVersion == segmentProjectDictDesc.getCurrentVersion()) {
        String patchPath = PathBuilder.patchPath(segmentProjectDictDesc.getSourceIdentify(),
                segmentProjectDictDesc.getCurrentVersion(), toVersion);
        if (loadDictPatch(patchPath) != null)
            paths.add(patchPath);
        //        }

        if ("true".equals(System.getProperty("dict.debug.enabled"))) {
            paths.add(PathBuilder.dataPath(segmentProjectDictDesc.getSourceIdentify(), toVersion));
        }
        if (segmentProjectDictDesc.getPatch() != null)
            paths.add(segmentProjectDictDesc.getPatch());

        logger.info("Get metadata with : " + segmentProjectDictDesc + ", version: " + toVersion + ", paths: "
                + String.join(",", paths));
        return paths;
    }

    private DictPatch loadDictPatch(String path) throws IOException {
        logger.info("Load project dictionary patch: " + path);
        ResourceStore store = getStore();
        return store.getResource(path, DictPatch.class, DICT_PATCH_SERIALIZER);
    }

    public ProjectDictionaryInfo getDictionary(String dictResPath) {
        if (dictionaryInfoMap.containsKey(dictResPath)) {
            return dictionaryInfoMap.get(dictResPath);
        } else {
            try {
                return loadDict(dictResPath);
            } catch (IOException e) {
                throw new RuntimeException("Load dictionary " + dictResPath + "failed!", e);
            }
        }
    }

    public ProjectDictionaryInfo getSpecialDictionary(SegmentProjectDictDesc desc, long specialVersion)
            throws IOException {
        if (desc == null) {
            return null;
        }
        Preconditions.checkArgument(desc.getCurrentVersion() <= specialVersion);
        ProjectDictionaryInfo projectDictionaryInfo = loadDictByVersion(desc.getSourceIdentify(), specialVersion);
        DictPatch patch = getSpecialPatch(desc, specialVersion);

        // Todo : Is nullable?
        if (projectDictionaryInfo == null) {
            ProjectDictionaryInfo nullProjectDictionaryInfo = new ProjectDictionaryInfo();
            nullProjectDictionaryInfo.setDictionaryObject(new DisguiseTrieDictionary<>(desc.getIdLength(), null, patch));
            return nullProjectDictionaryInfo;
        }
        logger.info("Get Special Dictionary: " + desc.getSourceIdentify() + " version : " + specialVersion);

        return ProjectDictionaryInfo.copy(projectDictionaryInfo,
                new DisguiseTrieDictionary<>(desc.getIdLength(), projectDictionaryInfo.getDictionaryObject(), patch));
    }

    public ProjectDictionaryInfo getSpecialDictWithOutPatch(SegmentProjectDictDesc desc, long specialVersion) {
        ProjectDictionaryInfo projectDictionaryInfo = loadDictByVersion(desc.getSourceIdentify(), specialVersion);
        if (projectDictionaryInfo == null) {
            throw new RuntimeException(" error dictionary");
        }
        logger.info("get dictionary: " + desc.getSourceIdentify());
        return ProjectDictionaryInfo.copy(projectDictionaryInfo,
                new DisguiseTrieDictionary<>(desc.getIdLength(), projectDictionaryInfo.getDictionaryObject(), null));
    }

    /**
     *
     * @param desc the segment project dict desc
     * @return     Combinatio dictionary  =  patch + max version dictionary
     * @throws IOException     ioe
     */
    public ProjectDictionaryInfo getCombinationDictionary(SegmentProjectDictDesc desc) throws IOException {
        long maxVersion = getMaxVersion(desc).getMaxVersion();
        return getSpecialDictionary(desc, maxVersion);
    }

    /**
     *
     * @param baseDir   s
     * @param version
     * @return
     */
    private ProjectDictionaryInfo loadDictByVersion(String baseDir, long version) {
        String dictResPath = PathBuilder.dataPath(baseDir, version);
        return getDictionary(dictResPath);
    }

    private synchronized ProjectDictionaryInfo loadDict(String dictResPath) throws IOException {
        ProjectDictionaryInfo load = doLoad(dictResPath, true);
        if (load != null)
            dictionaryInfoMap.put(dictResPath, load);
        return load;
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
    public SegmentProjectDictDesc append(String project, DictionaryInfo dictionaryInfo)
            throws ExecutionException, IOException {
        String sourceIdentify = PathBuilder.sourceIdentify(project, dictionaryInfo);
        MultiVersionControl mvc = mvcMap.get(sourceIdentify);
        long currentVersion = mvc.getCurrentVersion();
        // check contains
        if (currentVersion > -1) {
            // fetch current dict
            ProjectDictionaryInfo projectDictionaryInfo = loadDictByVersion(sourceIdentify, currentVersion);
            if (projectDictionaryInfo.getDictionaryObject().contains(dictionaryInfo.getDictionaryObject())) {
                logger.info("Dictionary " + sourceIdentify + "be contained version"
                        + projectDictionaryInfo.getDictionaryVersion());
                return new SegmentProjectDictDesc(sourceIdentify, currentVersion,
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

    private SegmentProjectDictDesc appendDictionary(String sourceIdentify, DictionaryInfo dictionaryInfo,
                                                    MultiVersionControl mvc, MultiVersionControl.VersionEntry versionEntry) throws IOException {
        checkInterrupted(sourceIdentify);

        logger.info("Append project dictionary with column: " + sourceIdentify);
        String s = eatAndUpgradeDictionary(dictionaryInfo, sourceIdentify, versionEntry.getVersion(), versionEntry);
        mvc.commit(versionEntry);
        return new SegmentProjectDictDesc(sourceIdentify, versionEntry.getVersion(), s,
                dictionaryInfo.getDictionaryObject().getSizeOfId());
    }

    private void versionCheckPoint(String sourceIdentify, MultiVersionControl.VersionEntry versionEntry,
            ProjectDictionaryVersionInfo projectDictionaryVersion, int sizeOfId) throws IOException {
        if (projectDictionaryVersion != null) {
            ProjectDictionaryVersionInfo copy = projectDictionaryVersion.copy();
            copy.setMaxVersion(versionEntry.getVersion());
            crud.save(copy);
        } else {
            crud.save(new ProjectDictionaryVersionInfo(sourceIdentify, versionEntry.getVersion(), sizeOfId));
        }
    }

    private SegmentProjectDictDesc createDictionary(String sourceIdentify, DictionaryInfo dictionaryInfo,
                                                    MultiVersionControl mvc, MultiVersionControl.VersionEntry versionEntry) throws IOException {
        checkInterrupted(sourceIdentify);

        logger.info("create project dictionary : " + sourceIdentify);
        ProjectDictionaryInfo warp = ProjectDictionaryInfo.wrap(dictionaryInfo, versionEntry.getVersion());
        saveDictionary(sourceIdentify, warp);
        mvc.commit(versionEntry);
        ProjectDictionaryVersionInfo versionInfo = versionCache.get(PathBuilder.versionKey(sourceIdentify));
        versionCheckPoint(sourceIdentify, versionEntry, versionInfo, dictionaryInfo.getDictionaryObject().getSizeOfId());
        return new SegmentProjectDictDesc(sourceIdentify, versionEntry.getVersion(),
                dictionaryInfo.getDictionaryObject().getSizeOfId());
    }

    private void saveDictionary(String sourceIdentify, ProjectDictionaryInfo dictionaryInfo) throws IOException {
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


        ByteArrayOutputStream buf2 = new ByteArrayOutputStream();
        DataOutputStream out2 = new DataOutputStream(buf);

        String sDictPath = PathBuilder.sDictPath(sourceIdentify, dictionaryInfo.getDictionaryVersion());
        Path f = new Path(sDictDir, new Path(sourceIdentify + "/" + dictionaryInfo.getDictionaryVersion() + "/data.sdict"));
        try (FSDataOutputStream out1 = workingFileSystem.create(f)) {
            SDict.wrap(dictionaryInfo.getDictionaryObject()).write(out1);
        }
    }

    //todo if equal
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
        versionCheckPoint(sourceIdentify, versionEntry, versionInfo, mergedDictionary.getDictionaryObject().getSizeOfId());

        if (originDict.getDictionaryObject().getSize() > 0) {
            return genSegmentDictionaryToProjectDictionaryMapping(originDict, sourceIdentify, mergedDictionary,
                    version);
        } else {
            return null;
        }
    }

    private String genSegmentDictionaryToProjectDictionaryMapping(DictionaryInfo originDict, String baseDir,
            DictionaryInfo mergedDictionary, long version) throws IOException {
        int[] offset = ProjectDictionaryHelper.genOffset(originDict, mergedDictionary);
        // different version has different patch
        String path = PathBuilder.segmentPatchPath(baseDir, originDict.getUuid() + "_" + version);
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
    public void releaseAll() {
        for (Map.Entry<String, MultiVersionControl> entry : mvcMap.asMap().entrySet()) {
            mvcMap.invalidate(entry.getKey());
            entry.getValue().clear();
        }
    }

    private void checkInterrupted(String sourceIdentify) {
        if (Thread.currentThread().isInterrupted()) {
            try {
                mvcMap.get(sourceIdentify).notifyAllThread();
            } catch (ExecutionException e) {
                logger.info("failed to find mvc  with column :" + sourceIdentify);
            }
            throw new RuntimeException(new InterruptedException("Interrupted build dictionary : " + sourceIdentify));
        }
    }
}
