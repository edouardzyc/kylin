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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.kv.RowKeyEncoderProvider;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CuboidSchedulerUtil;
import org.apache.kylin.engine.mr.common.NDCuboidBuilder;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author George Song (ysong1)
 * 
 */
public class NDCuboidMapper extends KylinMapper<Text, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(NDCuboidMapper.class);

    private Text outputKey = new Text();
    private String cubeName;
    private String segmentID;
    private CubeDesc cubeDesc;
    private CubeSegment cubeSegment;
    private CuboidScheduler cuboidScheduler;

    private int handleCounter;
    private int skipCounter;

    private RowKeySplitter rowKeySplitter;

    private NDCuboidBuilder ndCuboidBuilder;

    private long cuboidId;

    private RowKeyEncoderProvider rowKeyEncoderProvider;

    byte[] shardIdBytes = Bytes.toBytes((short) 0);

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        String cuboidModeName = context.getConfiguration().get(BatchConstants.CFG_CUBOID_MODE);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegmentById(segmentID);

        if (cubeSegment.getStorageType() == 99) {
            ndCuboidBuilder = new NDCuboidBuilder(cubeSegment);
            rowKeySplitter = new RowKeySplitter(cubeSegment);
        } else {
            // storage engine 100 will not load the dict to fetch the column length info
            // the cuboidId info will be parsed from the path
            String[] dirs = ((FileSplit)context.getInputSplit()).getPath().toString().split("/");
            cuboidId = Long.parseLong(dirs[dirs.length - 2]);
            logger.info("cuboidId : " + cuboidId);
            rowKeyEncoderProvider = new RowKeyEncoderProvider(cubeSegment);
        }

        // initialize CubiodScheduler
        cuboidScheduler = CuboidSchedulerUtil.getCuboidSchedulerByMode(cubeSegment, cuboidModeName);
    }



    @Override
    public void doMap(Text key, Text value, Context context) throws IOException, InterruptedException {
        if (cubeSegment.getStorageType() == 99) {
            cuboidId = rowKeySplitter.split(key.getBytes());
        }

        Cuboid parentCuboid = Cuboid.findForMandatory(cubeDesc, cuboidId);

        Collection<Long> myChildren = cuboidScheduler.getSpanningCuboid(cuboidId);

        // if still empty or null
        if (myChildren == null || myChildren.size() == 0) {
            context.getCounter(BatchConstants.MAPREDUCE_COUNTER_GROUP_NAME, "Skipped records").increment(1L);
            if (skipCounter++ % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
                logger.info("Skipping record with ordinal: " + skipCounter);
            }
            return;
        }

        context.getCounter(BatchConstants.MAPREDUCE_COUNTER_GROUP_NAME, "Processed records").increment(1L);

        if (handleCounter++ % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handling record with ordinal: " + handleCounter);
            logger.info("Parent cuboid: " + parentCuboid.getId() + "; Children: " + myChildren);
        }

        for (Long child : myChildren) {
            Cuboid childCuboid = Cuboid.findForMandatory(cubeDesc, child);
            if (cubeSegment.getStorageType() == 99) {
                Pair<Integer, ByteArray> result = ndCuboidBuilder.buildKey(parentCuboid, childCuboid, rowKeySplitter.getSplitBuffers());
                outputKey.set(result.getSecond().array(), 0, result.getFirst());
            } else {
                // build outputKey without rowKeySplitter and ndCuboidBuilder to reduce memory copy
                buildKey(outputKey, parentCuboid, childCuboid, key.getBytes());
            }
            context.write(outputKey, value);
        }

    }

    private void buildKey(Text outputKey, Cuboid parentCuboid, Cuboid childCuboid, byte[] bytes) {

        outputKey.clear();
        outputKey.append(shardIdBytes , 0, RowConstants.ROWKEY_SHARDID_LEN);
        outputKey.append(childCuboid.getBytes() , 0, RowConstants.ROWKEY_CUBOIDID_LEN);

        // rowkey columns
        int offset = 0;
        long mask = Long.highestOneBit(parentCuboid.getId());
        long parentCuboidId = parentCuboid.getId();
        long childCuboidId = childCuboid.getId();
        long parentCuboidIdActualLength = (long)Long.SIZE - Long.numberOfLeadingZeros(parentCuboid.getId());
        int index = 0;
        for (int i = 0; i < parentCuboidIdActualLength; i++) {
            if ((mask & parentCuboidId) > 0) {// if the this bit position equals
                // column
                TblColRef col = parentCuboid.getColumns().get(index);
                int colLength = cubeSegment.getColumnLengthMap().get(col.getIdentity());
                // 1
                if ((mask & childCuboidId) > 0) {// if the child cuboid has this
                    outputKey.append(bytes, offset, colLength);
                }
                index ++;
                offset += colLength;
            }
            mask = mask >> 1;
        }

        short childCuboidShardId = rowKeyEncoderProvider.getRowkeyEncoder(childCuboid).calculateShard(outputKey.getBytes());
        System.arraycopy(Bytes.toBytes(childCuboidShardId) , 0, outputKey.getBytes(), 0, RowConstants.ROWKEY_SHARDID_LEN);
    }
}
