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

package org.apache.kylin.rest.job;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.dict.project.SegProjectDict;
import org.apache.kylin.job.dao.ExecutableDao;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

public class MetadataCleanupJob {

    private static final Logger logger = LoggerFactory.getLogger(MetadataCleanupJob.class);

    public static final long NEW_RESOURCE_THREADSHOLD_MS = 12 * 3600 * 1000L; // 12 hour

    // ============================================================================

    final KylinConfig config;

    private List<String> garbageResources = Collections.emptyList();

    public MetadataCleanupJob() {
        this(KylinConfig.getInstanceFromEnv());
    }

    public MetadataCleanupJob(KylinConfig config) {
        this.config = config;
    }

    public List<String> getGarbageResources() {
        return garbageResources;
    }

    // function entrance
    public List<String> cleanup(boolean delete, int jobOutdatedDays) throws Exception {
        CubeManager cubeManager = CubeManager.getInstance(config);
        ResourceStore store = ResourceStore.getStore(config);
        long newResourceTimeCut = System.currentTimeMillis() - NEW_RESOURCE_THREADSHOLD_MS;

        List<String> toDeleteCandidates = Lists.newArrayList();

        // two level resources, snapshot tables and cube statistics
        for (String resourceRoot : new String[] { ResourceStore.SNAPSHOT_RESOURCE_ROOT,
                ResourceStore.CUBE_STATISTICS_ROOT }) {
            for (String dir : noNull(store.listResources(resourceRoot))) {
                for (String res : noNull(store.listResources(dir))) {
                    if (store.getResourceTimestamp(res) < newResourceTimeCut)
                        toDeleteCandidates.add(res);
                }
            }
        }

        // three level resources, only dictionaries
        for (String resourceRoot : new String[] { ResourceStore.DICT_RESOURCE_ROOT }) {
            for (String dir : noNull(store.listResources(resourceRoot))) {
                if (!dir.startsWith(ResourceStore.PROJECT_DICT_RESOURCE_ROOT)) {
                    for (String dir2 : noNull(store.listResources(dir))) {
                        for (String res : noNull(store.listResources(dir2))) {
                            if (store.getResourceTimestamp(res) < newResourceTimeCut)
                                toDeleteCandidates.add(res);
                        }
                    }
                }
            }
        }

        // project dict
        TreeMultimap<String, String> allProjectDict = TreeMultimap.create(Ordering.natural(), new Comparator<String>() {
            @Override
            // or version 15 will be front of version 2.
            // path like that: /dict/project_dict/data/default/DEFAULT.TEST_KYLIN_FACT/ONYL_ONE_VERSION/0
            public int compare(String s1, String s2) {
                Integer n1 = Integer.parseInt(s1.substring(StringUtils.lastOrdinalIndexOf(s1, "/", 1) + 1, s1.length()));
                Integer n2 = Integer.parseInt(s2.substring(StringUtils.lastOrdinalIndexOf(s2, "/", 1) + 1, s2.length()));
                return n1.compareTo(n2);
            }
        });

        NavigableSet<String> prjs = noNull(store.listResources(ResourceStore.PROJECT_DICT_RESOURCE_ROOT + "/data"));
        removeSpecificForder(prjs, "/metadata"); // exclude "metadata" folder
        for (String prj : prjs) {
            for (String tbl : noNull(store.listResources(prj))) {
                for (String col : noNull(store.listResources(tbl))) {
                    NavigableSet<String> versions = noNull(store.listResources(col));
                    removeSpecificForder(versions, "/segment"); // exclude "segment" folder
                    allProjectDict.putAll(col, versions);
                }
            }
        }

        // exclude resources in use
        Set<String> activeResources = Sets.newHashSet();

        for (CubeInstance cube : cubeManager.listAllCubes()) {
            for (CubeSegment segment : cube.getSegments()) {
                activeResources.addAll(segment.getSnapshotPaths());
                activeResources.addAll(segment.getDictionaryPaths());
                activeResources.add(segment.getStatisticsResourcePath());
            }
        }
        // remove last version
        NavigableMap<String, Collection<String>> map = allProjectDict.asMap();
        for (String colPath : map.keySet()) {
            NavigableSet<String> versions = (NavigableSet<String>) map.get(colPath);
            if (versions != null && versions.size() > 0) {
                String last = versions.last();
                activeResources.addAll(noNull(store.listResources(last)));
            }
        }

        for (Collection<String> versions : map.values()) {
            for (String version : versions) {
                for (String dict : noNull(store.listResources(version))) {
                    // check modify time.
                    if (store.getResourceTimestamp(dict) < newResourceTimeCut) {
                        toDeleteCandidates.add(dict);
                    }
                }
            }
        }

        toDeleteCandidates.removeAll(activeResources);

        // delete old and completed jobs
        long outdatedJobTimeCut = System.currentTimeMillis() - jobOutdatedDays * 24 * 3600 * 1000L;
        ExecutableDao executableDao = ExecutableDao.getInstance(config);
        List<ExecutablePO> allExecutable = executableDao.getJobs();
        for (ExecutablePO executable : allExecutable) {
            long lastModified = executable.getLastModified();
            String jobStatus = executableDao.getJobOutput(executable.getUuid()).getStatus();

            if (lastModified < outdatedJobTimeCut && (ExecutableState.SUCCEED.toString().equals(jobStatus)
                    || ExecutableState.DISCARDED.toString().equals(jobStatus))) {
                toDeleteCandidates.add(ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + executable.getUuid());
                toDeleteCandidates.add(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + executable.getUuid());

                for (ExecutablePO task : executable.getTasks()) {
                    toDeleteCandidates.add(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + task.getUuid());
                }
            }
        }

        garbageResources = cleanupConclude(delete, toDeleteCandidates);
        return garbageResources;
    }

    public static Set<String> getAllColsHasMVDict(CubeSegment segment) {
        Set<String> activePrjDictCol = Sets.newHashSet();
        Collection<SegProjectDict> prjDictDescs = segment.getProjectDictionaries();
        for (SegProjectDict prjDictDesc : prjDictDescs) {
            activePrjDictCol.add(prjDictDesc.getSourceIdentifier());
        }
        return activePrjDictCol;
    }

    private void removeSpecificForder(NavigableSet<String> paths, String toBeRmovedSuffix) {
        String toBeRemovePath = "";
        for (String p : paths) {
            if (p.endsWith(toBeRmovedSuffix)) {
                toBeRemovePath = p;
            }
        }
        paths.remove(toBeRemovePath);
    }

    private List<String> cleanupConclude(boolean delete, List<String> toDeleteResources) {
        if (toDeleteResources.isEmpty()) {
            logger.info("No metadata resource to clean up");
            return toDeleteResources;
        }

        logger.info(toDeleteResources.size() + " metadata resource to clean up");

        if (delete) {
            ResourceStore store = ResourceStore.getStore(config);
            for (String res : toDeleteResources) {
                logger.info("Deleting metadata " + res);
                try {
                    store.deleteResource(res);
                } catch (IOException e) {
                    logger.error("Failed to delete resource " + res, e);
                }
            }
        } else {
            for (String res : toDeleteResources) {
                logger.info("Dry run, pending delete metadata " + res);
            }
        }
        return toDeleteResources;
    }

    private NavigableSet<String> noNull(NavigableSet<String> list) {
        return (list == null) ? new TreeSet<String>() : list;
    }

}
