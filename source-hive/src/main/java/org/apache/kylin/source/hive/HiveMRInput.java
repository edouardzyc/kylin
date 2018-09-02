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

package org.apache.kylin.source.hive;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.FactInputSubstitute;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.common.HiveCommandExecutable;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class HiveMRInput implements IMRInput {

    private static final Logger logger = LoggerFactory.getLogger(HiveMRInput.class);
    public static final String PROJECT_INSTANCE_NAME = "projectName";

    public static String getTableNameForHCat(TableDesc table, String uuid) {
        String tableName = (table.isView()) ? table.getMaterializedName(uuid) : table.getName();
        String database = (table.isView()) ? KylinConfig.getInstanceFromEnv().getHiveDatabaseForIntermediateTable()
                : table.getDatabase();
        return String.format("%s.%s", database, tableName).toUpperCase();
    }

    @Override
    public IMRBatchCubingInputSide getBatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
        return new BatchCubingInputSide(flatDesc);
    }

    @Override
    public IMRTableInputFormat getTableInputFormat(TableDesc table, String uuid) {
        return new HiveTableInputFormat(getTableNameForHCat(table, uuid));
    }

    @Override
    public IMRBatchMergeInputSide getBatchMergeInputSide(ISegment seg) {
        return new IMRBatchMergeInputSide() {
            @Override
            public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow) {
                // doing nothing
            }
        };
    }

    public static class HiveTableInputFormat implements IMRTableInputFormat {
        final String dbName;
        final String tableName;

        /**
         * Construct a HiveTableInputFormat to read hive table.
         * @param fullQualifiedTableName "databaseName.tableName"
         */
        public HiveTableInputFormat(String fullQualifiedTableName) {
            String[] parts = HadoopUtil.parseHiveTableName(fullQualifiedTableName);
            dbName = parts[0];
            tableName = parts[1];
        }

        @Override
        public void configureJob(Job job) {
            try {
                job.getConfiguration().addResource("hive-site.xml");

                HCatInputFormat.setInput(job, dbName, tableName);
                job.setInputFormatClass(HCatInputFormat.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public List<String[]> parseMapperInput(Object mapperInput) {
            return Collections.singletonList(HiveTableReader.getRowAsStringArray((HCatRecord) mapperInput));
        }

    }

    public static class BatchCubingInputSide implements IMRBatchCubingInputSide {

        final protected IJoinedFlatTableDesc flatDesc;
        final protected String flatTableDatabase;
        final protected String hdfsWorkingDir;

        String hiveViewIntermediateTables = "";

        public BatchCubingInputSide(IJoinedFlatTableDesc flatDesc) {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            this.flatDesc = flatDesc;
            this.flatTableDatabase = config.getHiveDatabaseForIntermediateTable();
            this.hdfsWorkingDir = config.getHdfsWorkingDirectoryWithoutScheme(null);
        }

        @Override
        public void addStepPhase1_CreateFlatTable(DefaultChainedExecutable jobFlow) {
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            final KylinConfig cubeConfig = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(cubeName)
                    .getConfig();
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);

            // create flat table first
            addStepPhase1_DoCreateFlatTable(jobFlow);

            // then count and redistribute
            if (cubeConfig.isHiveRedistributeEnabled()) {
                if (flatDesc.getClusterBy() != null || flatDesc.getDistributedBy() != null) {
                    jobFlow.addTask(createRedistributeFlatHiveTableStep(hiveInitStatements, cubeName));
                }
            }

            // special for hive
            addStepPhase1_DoMaterializeLookupTable(jobFlow);
        }

        @Override
        public void addStepPhase1_DoCreateFlatTable(DefaultChainedExecutable jobFlow) {
            logger.info("addStepPhase1_DoCreateFlatTable");
            String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            String modelName = jobFlow.getParam("model_name");

            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);
            final String jobWorkingDir = getJobWorkingDir(jobFlow);

            jobFlow.addTask(createFlatHiveTableStep(hiveInitStatements, jobWorkingDir, cubeName, modelName));
        }

        protected void addStepPhase1_DoMaterializeLookupTable(DefaultChainedExecutable jobFlow) {
            final String hiveInitStatements = JoinedFlatTable.generateHiveInitStatements(flatTableDatabase);
            final String jobWorkingDir = getJobWorkingDir(jobFlow);

            AbstractExecutable task = createLookupHiveViewMaterializationStep(hiveInitStatements, jobWorkingDir,
                    jobFlow.getId());
            if (task != null) {
                jobFlow.addTask(task);
            }
        }

        protected String getJobWorkingDir(DefaultChainedExecutable jobFlow) {
            final KylinConfig cubeConfig = KylinConfig.getInstanceFromEnv();

            final String projectName = jobFlow.getParam(PROJECT_INSTANCE_NAME);
            return JobBuilderSupport.getJobWorkingDir(cubeConfig.getHdfsWorkingDirectoryWithoutScheme(projectName),
                    jobFlow.getId());
        }

        private AbstractExecutable createRedistributeFlatHiveTableStep(String hiveInitStatements, String cubeName) {
            RedistributeFlatHiveTableStep step = new RedistributeFlatHiveTableStep();
            step.setInitStatement(hiveInitStatements);
            step.setIntermediateTable(flatDesc.getTableName());
            step.setRedistributeDataStatement(JoinedFlatTable.generateRedistributeFlatTableStatement(flatDesc));
            CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            step.setName(ExecutableConstants.STEP_NAME_REDISTRIBUTE_FLAT_HIVE_TABLE);
            return step;
        }

        private HiveCommandExecutable createLookupHiveViewMaterializationStep(String hiveStatements,
                                                                              String jobWorkingDir, String uuid) {
            HiveCommandExecutable step = new HiveCommandExecutable();
            step.setName(ExecutableConstants.STEP_NAME_MATERIALIZE_HIVE_VIEW_IN_LOOKUP);

            KylinConfig kylinConfig = ((CubeSegment) flatDesc.getSegment()).getConfig();
            TableMetadataManager metadataManager = TableMetadataManager.getInstance(kylinConfig);
            final Set<TableDesc> lookupViewsTables = Sets.newHashSet();

            String prj = flatDesc.getDataModel().getProject();
            for (JoinTableDesc lookupDesc : flatDesc.getDataModel().getJoinTables()) {
                TableDesc tableDesc = metadataManager.getTableDesc(lookupDesc.getTable(), prj);
                if (lookupDesc.getKind() == DataModelDesc.TableKind.LOOKUP && tableDesc.isView()) {
                    lookupViewsTables.add(tableDesc);
                }
            }

            if (lookupViewsTables.size() == 0) {
                return null;
            }

            for (TableDesc lookUpTableDesc : lookupViewsTables) {
                String identity = lookUpTableDesc.getIdentity();
                if (lookUpTableDesc.isView()) {
                    String intermediate = lookUpTableDesc.getMaterializedName(uuid);
                    String materializeViewHql = materializeViewHql(intermediate, identity, jobWorkingDir);
                    hiveStatements = hiveStatements + materializeViewHql;
                    hiveViewIntermediateTables = hiveViewIntermediateTables + intermediate + ";";
                }
            }

            step.setStatement(hiveStatements);

            hiveViewIntermediateTables = hiveViewIntermediateTables.substring(0,
                    hiveViewIntermediateTables.length() - 1);

            return step;
        }

        // each append must be a complete hql.
        public static String materializeViewHql(String viewName, String tableName, String jobWorkingDir) {
            StringBuilder createIntermediateTableHql = new StringBuilder();
            createIntermediateTableHql.append("DROP TABLE IF EXISTS " + viewName + ";\n");
            createIntermediateTableHql.append("CREATE TABLE IF NOT EXISTS " + viewName + " LIKE " + tableName
                    + " LOCATION '" + jobWorkingDir + "/" + viewName + "';\n");
            createIntermediateTableHql.append("ALTER TABLE " + viewName + " SET TBLPROPERTIES('auto.purge'='true');\n");
            createIntermediateTableHql
                    .append("INSERT OVERWRITE TABLE " + viewName + " SELECT * FROM " + tableName + ";\n");
            return createIntermediateTableHql.toString();
        }

        private AbstractExecutable createFlatHiveTableStep(String hiveInitStatements, String jobWorkingDir,
                String cubeName, String modelName) {
            //from hive to hive
            final String dropTableHql = JoinedFlatTable.generateDropTableStatement(flatDesc);
            final String createTableHql = JoinedFlatTable.generateCreateTableStatement(flatDesc, jobWorkingDir);
            String insertDataHqls;
            if (flatDesc.getSegment() instanceof CubeSegment) {
                insertDataHqls = JoinedFlatTable.generateInsertDataStatement(flatDesc);
            } else {
                insertDataHqls = JoinedFlatTable.generateInsertPartialDataStatement(flatDesc);
            }

            CreateFlatHiveTableStep step = new CreateFlatHiveTableStep();
            step.setInitStatement(hiveInitStatements);
            step.setCreateTableStatement(dropTableHql + createTableHql + insertDataHqls);
            CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            CubingExecutableUtil.setSegmentId(flatDesc.getSegment().getUuid(), step.getParams());
            step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
            step.setWorkingDir(jobWorkingDir);

            if (cubeName != null) {
                CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            }

            if (modelName != null) {
                step.setParam("model_name", modelName);
            }

            return step;
        }

        @Override
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow) {
            final String cubeName = CubingExecutableUtil.getCubeName(jobFlow.getParams());
            final String jobWorkingDir = getJobWorkingDir(jobFlow);

            GarbageCollectionStep step = new GarbageCollectionStep();
            step.setName(ExecutableConstants.STEP_NAME_HIVE_CLEANUP);
            step.setIntermediateTableIdentity(getIntermediateTableIdentity());
            step.setExternalDataPath(JoinedFlatTable.getTableDir(flatDesc, jobWorkingDir));
            step.setHiveViewIntermediateTableIdentities(hiveViewIntermediateTables);
            CubingExecutableUtil.setCubeName(cubeName, step.getParams());
            CubingExecutableUtil.setSegmentId(flatDesc.getSegment().getUuid(), step.getParams());
            jobFlow.addTask(step);
        }

        @Override
        public IMRTableInputFormat getFlatTableInputFormat() {
            return new HiveTableInputFormat(getIntermediateTableIdentity());
        }

        private String getIntermediateTableIdentity() {
            return flatTableDatabase + "." + flatDesc.getTableName();
        }
    }

    public static class RedistributeFlatHiveTableStep extends AbstractExecutable {
        private final PatternedLogger stepLogger = new PatternedLogger(logger);

        private long computeRowCount(String database, String table) throws Exception {
            IHiveClient hiveClient = HiveClientFactory.getHiveClient();
            return hiveClient.getHiveTableRows(database, table);
        }

        private long getDataSize(String database, String table) throws Exception {
            IHiveClient hiveClient = HiveClientFactory.getHiveClient();
            long size = hiveClient.getHiveTableMeta(database, table).fileSize;
            return size;
        }

        private void redistributeTable(KylinConfig config, int numReducers) throws IOException {
            final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
            hiveCmdBuilder.overwriteHiveProps(config.getHiveConfigOverride());
            hiveCmdBuilder.addStatement(getInitStatement());
            hiveCmdBuilder.addStatement("set mapreduce.job.reduces=" + numReducers + ";\n");
            hiveCmdBuilder.addStatement("set hive.merge.mapredfiles=false;\n");
            hiveCmdBuilder.addStatement(getRedistributeDataStatement());
            final String cmd = hiveCmdBuilder.toString();

            stepLogger.log("Redistribute table, cmd: ");
            stepLogger.log(cmd);

            Pair<Integer, String> response = config.getCliCommandExecutor().execute(cmd, stepLogger);
            getManager().addJobInfo(getId(), stepLogger.getInfo());

            if (response.getFirst() != 0) {
                throw new RuntimeException("Failed to redistribute flat hive table");
            }
        }

        private KylinConfig getCubeSpecificConfig() {
            String cubeName = CubingExecutableUtil.getCubeName(getParams());
            CubeManager manager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = manager.getCube(cubeName);
            return cube.getConfig();
        }

        @Override
        protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
            KylinConfig config = getCubeSpecificConfig();
            String intermediateTable = getIntermediateTable();
            String database, tableName;
            if (intermediateTable.indexOf(".") > 0) {
                database = intermediateTable.substring(0, intermediateTable.indexOf("."));
                tableName = intermediateTable.substring(intermediateTable.indexOf(".") + 1);
            } else {
                database = config.getHiveDatabaseForIntermediateTable();
                tableName = intermediateTable;
            }

            try {
                long rowCount = computeRowCount(database, tableName);
                logger.debug("Row count of table '" + intermediateTable + "' is " + rowCount);
                if (rowCount == 0) {
                    if (!config.isEmptySegmentAllowed()) {
                        stepLogger.log("Detect upstream hive table is empty, "
                                + "fail the job because \"kylin.job.allow-empty-segment\" = \"false\"");
                        return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
                    } else {
                        return new ExecuteResult(ExecuteResult.State.SUCCEED,
                                "Row count is 0, no need to redistribute");
                    }
                }

                int mapperInputRows = config.getHadoopJobMapperInputRows();

                int numReducers = Math.round(rowCount / ((float) mapperInputRows));
                numReducers = Math.max(1, numReducers);
                numReducers = Math.min(numReducers, config.getHadoopJobMaxReducerNumber());

                stepLogger.log("total input rows = " + rowCount);
                stepLogger.log("expected input rows per mapper = " + mapperInputRows);
                stepLogger.log("num reducers for RedistributeFlatHiveTableStep = " + numReducers);

                redistributeTable(config, numReducers);
                long dataSize = getDataSize(database, tableName);
                getManager().addJobInfo(getId(), ExecutableConstants.HDFS_BYTES_WRITTEN, "" + dataSize);
                return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());

            } catch (Exception e) {
                logger.error("job:" + getId() + " execute finished with exception", e);
                return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog(), e);
            }
        }

        public void setInitStatement(String sql) {
            setParam("HiveInit", sql);
        }

        public String getInitStatement() {
            return getParam("HiveInit");
        }

        public void setRedistributeDataStatement(String sql) {
            setParam("HiveRedistributeData", sql);
        }

        public String getRedistributeDataStatement() {
            return getParam("HiveRedistributeData");
        }

        public String getIntermediateTable() {
            return getParam("intermediateTable");
        }

        public void setIntermediateTable(String intermediateTable) {
            setParam("intermediateTable", intermediateTable);
        }
    }

    public static class GarbageCollectionStep extends AbstractExecutable {
        private static final Logger logger = LoggerFactory.getLogger(GarbageCollectionStep.class);

        @Override
        protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
            KylinConfig config = context.getConfig();
            StringBuffer output = new StringBuffer();
            try {
                output.append(cleanUpIntermediateFlatTable(config));
                output.append(cleanUpHiveViewIntermediateTables(config));
                tearDownFactInputSubstituteIfNeeded(context);
            } catch (IOException e) {
                logger.error("job:" + getId() + " execute finished with exception", e);
                return ExecuteResult.createError(e);
            }

            return new ExecuteResult(ExecuteResult.State.SUCCEED, output.toString());
        }

        private void tearDownFactInputSubstituteIfNeeded(ExecutableContext context) throws IOException {
            String cubeName = CubingExecutableUtil.getCubeName(getParams());
            // param cubeName is added, do a check for compatibility with the job/executable created by older KE
            if (StringUtils.isNotBlank(cubeName)) {
                CubeInstance cube = CubeManager.getInstance(context.getConfig()).getCube(cubeName);
                String segId = CubingExecutableUtil.getSegmentId(getParams());
                CubeSegment seg = cube.getSegmentById(segId);
                FactInputSubstitute sub = FactInputSubstitute.getInstance(seg);
                if (sub != null) {
                    logger.debug("Calling FactInputSubstitute.tearDownFactTableAfterBuild() on " + sub);
                    sub.tearDownFactTableAfterBuild();
                }
            }
        }

        private String cleanUpIntermediateFlatTable(KylinConfig config) throws IOException {
            StringBuffer output = new StringBuffer();
            final String hiveTable = this.getIntermediateTableIdentity();
            if (config.isHiveKeepFlatTable() == false && StringUtils.isNotEmpty(hiveTable)) {
                final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
                hiveCmdBuilder.addStatement("USE " + config.getHiveDatabaseForIntermediateTable() + ";");
                hiveCmdBuilder.addStatement("DROP TABLE IF EXISTS  " + hiveTable + ";");
                config.getCliCommandExecutor().execute(hiveCmdBuilder.build());
                output.append("Hive table " + hiveTable + " is dropped. \n");
                rmdirOnHDFS(getExternalDataPath());
                output.append(
                        "Hive table " + hiveTable + " external data path " + getExternalDataPath() + " is deleted. \n");
            }
            return output.toString();
        }

        private String cleanUpHiveViewIntermediateTables(KylinConfig config) throws IOException {
            StringBuffer output = new StringBuffer();
            final String oldHiveViewIntermediateTables = this.getHiveViewIntermediateTableIdentities();

            if (StringUtils.isNotEmpty(oldHiveViewIntermediateTables)) {
                final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
                hiveCmdBuilder.addStatement("USE " + config.getHiveDatabaseForIntermediateTable() + ";");
                for (String oldHiveViewIntermediateTable : oldHiveViewIntermediateTables.split(";")) {
                    hiveCmdBuilder.addStatement("DROP TABLE IF EXISTS " + oldHiveViewIntermediateTable + ";");
                }
                config.getCliCommandExecutor().execute(hiveCmdBuilder.build());
                output.append("Hive table(s) " + oldHiveViewIntermediateTables + " are dropped. \n");
            }
            return output.toString();
        }

        private void rmdirOnHDFS(String path) throws IOException {
            Path externalDataPath = new Path(path);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (fs.exists(externalDataPath)) {
                fs.delete(externalDataPath, true);
            }
        }

        public void setIntermediateTableIdentity(String tableIdentity) {
            setParam("oldHiveTable", tableIdentity);
        }

        private String getIntermediateTableIdentity() {
            return getParam("oldHiveTable");
        }

        public void setExternalDataPath(String externalDataPath) {
            setParam("externalDataPath", externalDataPath);
        }

        private String getExternalDataPath() {
            return getParam("externalDataPath");
        }

        public void setHiveViewIntermediateTableIdentities(String tableIdentities) {
            setParam("oldHiveViewIntermediateTables", tableIdentities);
        }

        public String getHiveViewIntermediateTableIdentities() {
            return getParam("oldHiveViewIntermediateTables");
        }
    }
}
