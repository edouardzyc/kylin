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

package org.apache.kylin.tool;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.ZipFileUtils;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.hybrid.HybridManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * companion tool for CubeMetaExtractor, ingest the extracted cube meta into another metadata store
 *
 * TODO: only support ingest cube now
 * TODO: ingest job history
 */
public class CubeMetaIngester extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(CubeMetaIngester.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_SRC = OptionBuilder.withArgName("srcPath").hasArg().isRequired(true)
            .withDescription("specify the path to the extracted cube metadata zip file").create("srcPath");

    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(true)
            .withDescription("specify the target project for the new cubes").create("project");

    @SuppressWarnings("static-access")
    private static final Option OPTION_FORCE_INGEST = OptionBuilder.withArgName("forceIngest").hasArg()
            .isRequired(false)
            .withDescription(
                    "skip the target cube, model and table check and ingest by force. Use in caution because it might break existing cubes! Suggest to backup metadata store first")
            .create("forceIngest");

    @SuppressWarnings("static-access")
    private static final Option OPTION_OVERWRITE_TABLES = OptionBuilder.withArgName("overwriteTables").hasArg()
            .isRequired(false)
            .withDescription(
                    "If table meta conflicts, overwrite the one in metadata store with the one in srcPath. Use in caution because it might break existing cubes! Suggest to backup metadata store first")
            .create("overwriteTables");

    @SuppressWarnings("static-access")
    private static final Option OPTION_RESTORE_TYPE = OptionBuilder.withArgName("restoreType").hasArg().isRequired(true)
            .withDescription("specify restore project or cube").create("restoreType");

    private static final String PROJECT_TYPE = "project";

    protected KylinConfig kylinConfig;

    protected Set<String> requiredResources = Sets.newLinkedHashSet();
    protected String targetProjectName;
    private boolean overwriteTables = false;
    protected boolean forceIngest = false;
    private Map<String, List<String>> conflictedResources = new HashMap<>();
    private String restoreType;
    private static String[] defaultProsKeys = new String[] { "kylin.query.force-limit", "kylin.source.default" };

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_SRC);
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_FORCE_INGEST);
        options.addOption(OPTION_OVERWRITE_TABLES);
        options.addOption(OPTION_RESTORE_TYPE);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        kylinConfig = KylinConfig.getInstanceFromEnv();

        if (optionsHelper.hasOption(OPTION_FORCE_INGEST)) {
            forceIngest = Boolean.valueOf(optionsHelper.getOptionValue(OPTION_FORCE_INGEST));
        }

        if (optionsHelper.hasOption(OPTION_OVERWRITE_TABLES)) {
            overwriteTables = Boolean.valueOf(optionsHelper.getOptionValue(OPTION_OVERWRITE_TABLES));
        }

        targetProjectName = optionsHelper.getOptionValue(OPTION_PROJECT);

        restoreType = optionsHelper.getOptionValue(OPTION_RESTORE_TYPE);

        String srcPath = optionsHelper.getOptionValue(OPTION_SRC);
        if (!srcPath.endsWith(".zip")) {
            throw new IllegalArgumentException(OPTION_SRC.getArgName() + " has to be a zip file");
        }
        File zipFile = new File(srcPath);
        if (zipFile.isDirectory() || !zipFile.exists()) {
            throw new IllegalArgumentException(OPTION_SRC.getArgName() + " file does does exist");
        }

        File tempFolder = File.createTempFile("_unzip", "folder");
        tempFolder.deleteOnExit();
        tempFolder.delete();
        tempFolder.mkdir();
        ZipFileUtils.decompressZipfileToDirectory(srcPath, tempFolder);

        File[] subFolders = tempFolder.listFiles();
        if (subFolders == null) {
            throw new IllegalStateException("Invalid source package.");
        }

        KylinConfig srcConfig;
        if (subFolders.length == 1) {
            srcConfig = KylinConfig.createInstanceFromUri(subFolders[0].getAbsoluteFile().getAbsolutePath());

        } else {
            srcConfig = KylinConfig.createInstanceFromUri(tempFolder.getAbsoluteFile().getAbsolutePath());
        }
        injest(srcConfig);
    }

    protected void injest(KylinConfig srcConfig) throws IOException {
        TableMetadataManager srcMetadataManager = TableMetadataManager.getInstance(srcConfig);
        DataModelManager srcModelManager = DataModelManager.getInstance(srcConfig);
        HybridManager srcHybridManager = HybridManager.getInstance(srcConfig);
        CubeManager srcCubeManager = CubeManager.getInstance(srcConfig);
        CubeDescManager srcCubeDescManager = CubeDescManager.getInstance(srcConfig);

        checkAndMark(srcMetadataManager, srcModelManager, srcHybridManager, srcCubeManager, srcCubeDescManager,
                srcConfig);
        ResourceTool.copy(srcConfig, kylinConfig, Lists.newArrayList(requiredResources));

        // clear the cache
        Broadcaster.getInstance(kylinConfig).notifyClearAll();

        ProjectManager projectManager = ProjectManager.getInstance(kylinConfig);
        TableMetadataManager metaMgr = TableMetadataManager.getInstance(kylinConfig);

        for (TableDesc tableDesc : srcMetadataManager.listAllTables(null)) {
            logger.info("add " + tableDesc + " to " + targetProjectName);
            TableDesc table = metaMgr.getTableDesc(tableDesc.getIdentity(), targetProjectName);
            if (table == null) {
                tableDesc.setLastModified(0);
            } else {
                tableDesc.setLastModified(table.getLastModified());
            }
            metaMgr.saveSourceTable(tableDesc, targetProjectName);
            projectManager.addTableDescToProject(Lists.newArrayList(tableDesc.getIdentity()).toArray(new String[0]),
                    targetProjectName);
        }

        for (DataModelDesc model : srcModelManager.listDataModels()) {
            logger.info("add " + model + " to " + targetProjectName);
            projectManager.addModelToProject(model.getName(), targetProjectName);
        }

        for (CubeInstance cube : srcCubeManager.listAllCubes()) {
            logger.info("add " + cube + " to " + targetProjectName);
            //Since there are no projects in sourceConfig, cube.getModel().getName() causes NPE.
            projectManager.addModelToProject(cube.getDescriptor().getModelName(), targetProjectName);
            projectManager.moveRealizationToProject(RealizationType.CUBE, cube.getName(), targetProjectName, null);
        }

    }

    protected void checkAndMark(TableMetadataManager srcMetadataManager, DataModelManager srcModelManager,
            HybridManager srcHybridManager, CubeManager srcCubeManager, CubeDescManager srcCubeDescManager,
            KylinConfig srcConfig) {
        if (srcHybridManager.listHybridInstances().size() > 0) {
            throw new IllegalStateException("Does not support ingest hybrid yet");
        }

        ProjectManager projectManager = ProjectManager.getInstance(kylinConfig);
        ProjectInstance targetProject = projectManager.getProject(targetProjectName);
        if (targetProject == null) {
            if (PROJECT_TYPE.equals(restoreType)) {
                ProjectManager srcProjectManager = ProjectManager.getInstance(srcConfig);
                ProjectInstance srcProject = srcProjectManager.getProject(targetProjectName);
                if (srcProject == null) {
                    throw new IllegalStateException(
                            "Target project does not exist in source metadata: " + targetProjectName);
                }
                try {
                    projectManager.createProject(targetProjectName, srcProject.getOwner(), srcProject.getDescription(),
                            srcProject.getOverrideKylinProps());
                } catch (IOException e) {
                    logger.error(
                            "create target project from source metadata to  target metadata error :" + e.getMessage(),
                            e);
                    throw new IllegalStateException(
                            "create target project from source metadata to  target metadata : " + targetProjectName);
                }
            } else {
                throw new IllegalStateException(
                        "Target project does not exist in target metadata: " + targetProjectName);
            }
        }

        TableMetadataManager metadataManager = TableMetadataManager.getInstance(kylinConfig);
        List<String> conflictedTables = new ArrayList<>();
        List<String> conflictedModels = new ArrayList<>();
        List<String> conflictedCubes = new ArrayList<>();

        for (TableDesc tableDesc : srcMetadataManager.listAllTables(null)) {
            TableDesc existing = metadataManager.getTableDesc(tableDesc.getIdentity(), targetProjectName);

            if (existing != null) {
                conflictedTables.add(tableDesc.getName());

                if (!existing.equals(tableDesc)) {
                    logger.info("Table {} already has a different version in target metadata store",
                            tableDesc.getIdentity());
                    logger.info("Existing version: " + existing);
                    logger.info("New version: " + tableDesc);

                    if (!forceIngest && !overwriteTables) {
                        throw new IllegalStateException("Table already exists with a different version: "
                                + tableDesc.getIdentity()
                                + ". Consider adding -overwriteTables option to force overwriting (with caution)");
                    } else {
                        logger.warn("Overwriting the old table desc: " + tableDesc.getIdentity());
                    }
                }
            }
            requiredResources.add(tableDesc.getResourcePath());
        }

        DataModelManager modelManager = DataModelManager.getInstance(kylinConfig);
        for (DataModelDesc dataModelDesc : srcModelManager.listDataModels()) {
            DataModelDesc existing = modelManager.getDataModelDesc(dataModelDesc.getName());
            if (existing != null) {
                conflictedModels.add(dataModelDesc.getName());
                if (!forceIngest || !existing.getProject().equals(targetProject.getName())) {
                    System.out.println("The model " + dataModelDesc.getName()
                            + " cannot exist in multiple projects, please resolve the conflicts. ");
                    throw new IllegalStateException("The model " + dataModelDesc.getName()
                            + " cannot exist in multiple projects, please resolve the conflicts. ");
                } else {
                    System.out.println("Overwriting the old model desc: " + dataModelDesc.getName());
                    logger.warn("Overwriting the old model desc: " + dataModelDesc.getName());
                }
            }
            requiredResources.add(DataModelDesc.concatResourcePath(dataModelDesc.getName()));
        }

        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        for (CubeInstance cube : srcCubeManager.listAllCubes()) {
            CubeInstance existing = cubeManager.getCube(cube.getName());
            if (existing != null) {
                conflictedCubes.add(cube.getDisplayName());
                Segments segments = existing.getSegments();
                if (segments.size() != 0) {
                    System.out.println("Please purge data of " + cube.getName() + " at first.");
                    throw new IllegalStateException("Please purge data of " + cube.getName() + " at first.");
                }
                // check they are in same model.
                if (!forceIngest
                        || !cube.getDescriptor().getModelName().equals(existing.getDescriptor().getModelName())) {
                    System.out.println("The cube " + cube.getName()
                            + " cannot exist in multiple models, please resolve the conflicts. ");
                    throw new IllegalStateException("The cube " + cube.getName()
                            + " cannot exist in multiple models, please resolve the conflicts. ");
                } else {
                    System.out.println("Overwriting the old cube : " + cube.getName());
                    logger.warn("Overwriting the old cube : " + cube.getName());
                }
            }
            requiredResources.add(CubeInstance.concatResourcePath(cube.getName()));
        }

        CubeDescManager cubeDescManager = CubeDescManager.getInstance(kylinConfig);
        for (CubeDesc cubeDesc : srcCubeDescManager.listAllDesc()) {
            CubeDesc existing = cubeDescManager.getCubeDesc(cubeDesc.getName());
            if (existing != null) {
                Segments segments = cubeManager.getCube(cubeDesc.getName()).getSegments();
                if (segments.size() != 0) {
                    System.out.println("Please purge data of " + cubeDesc.getName() + " at first.");
                    throw new IllegalStateException("Please purge data of " + cubeDesc.getName() + " at first.");
                }
                // check they are in same model.
                if (!forceIngest || !cubeDesc.getModelName().equals(existing.getModelName())) {
                    System.out.println("The cube" + cubeDesc.getName()
                            + "cannot exist in multiple models, please resolve the conflicts. ");
                    throw new IllegalStateException("The cube" + cubeDesc.getName()
                            + " cannot exist in multiple models, please resolve the conflicts. ");
                } else {
                    System.out.println("Overwriting the old cube desc: " + cubeDesc.getName());
                    logger.warn("Overwriting the old cube desc: " + cubeDesc.getName());
                }
            }
            requiredResources.add(CubeDesc.concatResourcePath(cubeDesc.getName()));
        }

        conflictedResources.put("table", conflictedTables);
        conflictedResources.put("model", conflictedModels);
        conflictedResources.put("cube", conflictedCubes);
    }

    public Map<String, List<String>> getConflictedResources() {
        return conflictedResources;
    }

    public static void main(String[] args) {
        CubeMetaIngester extractor = new CubeMetaIngester();
        extractor.execute(args);
    }

}
