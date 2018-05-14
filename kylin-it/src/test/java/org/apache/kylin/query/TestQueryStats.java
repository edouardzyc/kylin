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

package org.apache.kylin.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryContext.CubeSegmentStatistics;
import org.apache.kylin.common.QueryContext.CubeSegmentStatisticsResult;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

public class TestQueryStats {

    @JsonProperty("realizations")
    private List<RealizationStats> realizationStatsList;
    
    public TestQueryStats() {
    }
    
    public TestQueryStats(KylinConfig config, String project, QueryContext queryContext) {
        realizationStatsList = Lists.newArrayList();
        for (CubeSegmentStatisticsResult cssResult : queryContext.getCubeSegmentStatisticsResultList()) {
            RealizationStats rStats = new RealizationStats();
            realizationStatsList.add(rStats);
            rStats.queryType = cssResult.getQueryType();
            rStats.realization = cssResult.getRealization();
            rStats.realizationType = cssResult.getRealizationType();
            if (cssResult.getCubeSegmentStatisticsMap() == null) {
                continue;
            }
            for (Entry<String, Map<String, CubeSegmentStatistics>> cubeEntry : cssResult
                    .getCubeSegmentStatisticsMap().entrySet()) {
                for (Entry<String, CubeSegmentStatistics> segmentEntry : cubeEntry.getValue().entrySet()) {
                    CubeSegmentStatistics css = segmentEntry.getValue();
                    CubeDesc cubeDesc = CubeDescManager.getInstance(config).getCubeDesc(css.getCubeName());
                    rStats.sourceCuboidCols = translateIdToColumns(cubeDesc, css.getSourceCuboidId());
                    rStats.targetCuboidCols = translateIdToColumns(cubeDesc, css.getTargetCuboidId());
                    break;
                }
                break;
            }
        }
    }


    private List<String> translateIdToColumns(CubeDesc cubeDesc, long cuboidID) {
        List<String> dimesnions = Lists.newArrayList();
        if (cubeDesc == null) {
            return dimesnions;
        }
        RowKeyColDesc[] allColumns = cubeDesc.getRowkey().getRowKeyColumns();
        for (int i = 0; i < allColumns.length; i++) {
            long bitmask = 1L << allColumns[i].getBitIndex();
            if ((cuboidID & bitmask) != 0) {
                TblColRef colRef = allColumns[i].getColRef();
                dimesnions.add(colRef.getIdentity());
            }
        }
        return dimesnions;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TestQueryStats)) {
            return false;
        }
        TestQueryStats other = (TestQueryStats) obj;
        return this.realizationStatsList.equals(other.realizationStatsList);
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + realizationStatsList.hashCode();
        return result;
    }
    
    public static class RealizationStats implements Serializable {
        protected static final long serialVersionUID = 1L;

        @JsonProperty("query_type")
        private String queryType;
        @JsonProperty("realization")
        private String realization;
        @JsonProperty("realization_type")
        private String realizationType;
        @JsonProperty("source_cuboid_cols")
        private List<String> sourceCuboidCols = Lists.newArrayList();;
        @JsonProperty("target_cuboid_cols")
        private List<String> targetCuboidCols = Lists.newArrayList();;
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof RealizationStats)) {
                return false;
            }
            RealizationStats other = (RealizationStats) obj;
            if (!StringUtils.equals(this.queryType, other.queryType)) {
                return false;
            }
            if (!StringUtils.equals(this.realization, other.realization)) {
                return false;
            }
            if (!StringUtils.equals(this.realizationType, other.realizationType)) {
                return false;
            }
            if (!(this.sourceCuboidCols.equals(other.sourceCuboidCols))) {
                return false;
            }
            if (!(this.targetCuboidCols.equals(other.targetCuboidCols))) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = 0;
            result = 31 * result + queryType.hashCode();
            result = 31 * result + realization.hashCode();
            result = 31 * result + realizationType.hashCode();
            result = 31 * result + sourceCuboidCols.hashCode();
            result = 31 * result + targetCuboidCols.hashCode();
            return result;
        }
    }
}
