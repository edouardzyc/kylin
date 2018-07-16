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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.HiveCmdBuilder;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.FactInputSubstitute;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class CreateFlatHiveTableStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(CreateFlatHiveTableStep.class);
    protected final PatternedLogger stepLogger = new PatternedLogger(logger);
    private static final Pattern HDFS_LOCATION = Pattern.compile("LOCATION \'(.*)\';");

    protected void createFlatHiveTable(KylinConfig config) throws IOException {
        if (config.getHiveTableDirCreateFirst()) {
            // Create work dir to avoid hive create it,
            // the difference is that the owners are different.
            checkAndCreateWorkDir(getWorkingDir());
        } else {
            logger.info("Skip crate hive dir first.");
        }
        final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder();
        hiveCmdBuilder.overwriteHiveProps(config.getHiveConfigOverride());
        hiveCmdBuilder.addStatement(getInitStatement());
        hiveCmdBuilder.addStatement(getCreateTableStatement());
        final String cmd = hiveCmdBuilder.toString();

        stepLogger.log("Create and distribute table, cmd: ");
        stepLogger.log(cmd);

        Pair<Integer, String> response = config.getCliCommandExecutor().execute(cmd, stepLogger);
        Map<String, String> info = stepLogger.getInfo();

        //get the flat Hive table size
        Matcher matcher = HDFS_LOCATION.matcher(cmd);
        if (matcher.find()) {
            String hiveFlatTableHdfsUrl = matcher.group(1);
            long size = getFileSize(hiveFlatTableHdfsUrl);
            info.put(ExecutableConstants.HDFS_BYTES_WRITTEN, "" + size);
            logger.info("HDFS_Bytes_Writen: " + size);
        }
        getManager().addJobInfo(getId(), info);
        if (response.getFirst() != 0) {
            throw new RuntimeException("Failed to create flat hive table, error code " + response.getFirst());
        }
    }

    private long getFileSize(String hdfsUrl) throws IOException {
        Path path = new Path(hdfsUrl);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        ContentSummary contentSummary = fs.getContentSummary(path);
        long length = contentSummary.getLength();
        return length;
    }

    private CubeInstance getCube(ExecutableContext context) {
        String cubeName = CubingExecutableUtil.getCubeName(getParams());
        if (StringUtils.isEmpty(cubeName)) {
            return null;
        }
        CubeManager manager = CubeManager.getInstance(context.getConfig());
        return manager.getCube(cubeName);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig config = context.getConfig();
        CubeInstance cube = getCube(context);
        if (cube != null) {
            config = cube.getConfig();
        }

        try {
            setupFactInputSubstituteIfNeeded(context);
            createFlatHiveTable(config);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());
        } catch (Exception e) {
            logger.error("job:" + getId() + " execute finished with exception", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog(), e);
        }
    }

    private void setupFactInputSubstituteIfNeeded(ExecutableContext context) throws IOException {
        CubeInstance cube = getCube(context);
        if (cube == null) {
            return;
        }
        String segId = CubingExecutableUtil.getSegmentId(getParams());
        CubeSegment seg = cube.getSegmentById(segId);
        FactInputSubstitute sub = FactInputSubstitute.getInstance(seg);
        if (sub != null) {
            logger.debug("Calling FactInputSubstitute.setupFactTableBeforeBuild() on " + sub);
            sub.setupFactTableBeforeBuild();
        }
    }

    public void setInitStatement(String sql) {
        setParam("HiveInit", sql);
    }

    public String getInitStatement() {
        return getParam("HiveInit");
    }

    public void setCreateTableStatement(String sql) {
        setParam("HiveRedistributeData", sql);
    }

    public String getCreateTableStatement() {
        return getParam("HiveRedistributeData");
    }

    public void setWorkingDir(String workingDir) {
        setParam("WorkingDir", workingDir);
    }

    public String getWorkingDir() {
        return getParam("WorkingDIr");
    }

    private void checkAndCreateWorkDir(String jobWorkingDir) {
        try {
            Path path = new Path(jobWorkingDir);
            FileSystem fileSystem = HadoopUtil.getFileSystem(path);
            if (!fileSystem.exists(path)) {
                logger.info("Create jobWorkDir : " + jobWorkingDir);
                fileSystem.mkdirs(path);
            }
            logger.info("File exists :" + jobWorkingDir);
        } catch (IOException e) {
            logger.error("Could not create lookUp table dir : " + jobWorkingDir);
        }
    }
}
