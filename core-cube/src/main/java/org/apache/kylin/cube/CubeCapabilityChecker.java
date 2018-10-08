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

package org.apache.kylin.cube;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.util.CubeSnapshotChecker;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.basic.BasicMeasureType;
import org.apache.kylin.metadata.filter.UDF.MassInTupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.CapabilityResult.CapabilityInfluence;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 */
public class CubeCapabilityChecker {
    private static final Logger logger = LoggerFactory.getLogger(CubeCapabilityChecker.class);

    public static CapabilityResult check(CubeInstance cube, SQLDigest digest) {
        CapabilityResult result = new CapabilityResult();
        result.capable = false;

        // match joins is ensured at model select

        // dimensions & measures
        Collection<TblColRef> dimensionColumns = getDimensionColumns(digest);
        Collection<FunctionDesc> aggrFunctions = digest.aggregations;
        Collection<TblColRef> unmatchedDimensions = unmatchedDimensions(dimensionColumns, cube);
        Collection<FunctionDesc> unmatchedAggregations = unmatchedAggregations(aggrFunctions, cube);

        // try custom measure types
        tryCustomMeasureTypes(unmatchedDimensions, unmatchedAggregations, digest, cube, result);

        //more tricks
        String rootFactTable = cube.getRootFactTable();
        if (rootFactTable.equals(digest.factTable)) {
            //for query-on-facttable
            //1. dimension as measure

            if (!unmatchedAggregations.isEmpty()) {
                removeUnmatchedGroupingAgg(unmatchedAggregations);
                tryDimensionAsMeasures(unmatchedAggregations, result,
                        cube.getDescriptor().listDimensionColumnsIncludingDerived(), true);
            }
        } else {
            //for non query-on-facttable
            CubeSnapshotChecker snapshotChecker = (CubeSnapshotChecker) ClassUtil
                    .newInstance(cube.getConfig().getSnapshotChecker());
            if (snapshotChecker.doesCubeHasSnapshot(cube, digest.factTable)) {
                Set<TblColRef> dimCols = Sets.newHashSet(cube.getModel().findFirstTable(digest.factTable).getColumns());

                //1. all aggregations on lookup table can be done. For distinct count, mark them all DimensionAsMeasures
                // so that the measure has a chance to be upgraded to DimCountDistinctMeasureType in org.apache.kylin.metadata.model.FunctionDesc#reInitMeasureType
                if (!unmatchedAggregations.isEmpty()) {
                    Iterator<FunctionDesc> itr = unmatchedAggregations.iterator();
                    while (itr.hasNext()) {
                        FunctionDesc functionDesc = itr.next();
                        if (dimCols.containsAll(functionDesc.getParameter().getColRefs())) {
                            itr.remove();
                        }
                    }
                }
                tryDimensionAsMeasures(Lists.newArrayList(aggrFunctions), result, dimCols, false);

                //2. more "dimensions" contributed by snapshot
                if (!unmatchedDimensions.isEmpty()) {
                    unmatchedDimensions.removeAll(dimCols);
                }
            }
        }

        //if not need accurate count result and there is not a matched count measure of specific column,
        //count(col) will be replaced with the count(*)
        if (CollectionUtils.isNotEmpty(unmatchedAggregations) && cube.getConfig().isReplaceColCountWithCountStar()) {
            Iterator<FunctionDesc> it = unmatchedAggregations.iterator();
            while (it.hasNext()) {
                if (it.next().isCount())
                    it.remove();
            }
        }

        if (!unmatchedDimensions.isEmpty()) {
            logger.info("Exclude cube " + cube.getName() + " because unmatched dimensions: " + unmatchedDimensions);
            result.incapableCause = CapabilityResult.IncapableCause.unmatchedDimensions(unmatchedDimensions);
            return result;
        }

        if (!unmatchedAggregations.isEmpty()) {
            logger.info("Exclude cube " + cube.getName() + " because unmatched aggregations: " + unmatchedAggregations);
            result.incapableCause = CapabilityResult.IncapableCause.unmatchedAggregations(unmatchedAggregations);
            return result;
        }

        if (cube.getStorageType() == IStorageAware.ID_HBASE
                && MassInTupleFilter.containsMassInTupleFilter(digest.filter)) {
            logger.info(
                    "Exclude cube " + cube.getName() + " because only v2 storage + v2 query engine supports massin");
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.UNSUPPORT_MASSIN);
            return result;
        }

        if (digest.limitPrecedesAggr) {
            logger.info("Exclude cube " + cube.getName() + " because there's limit preceding aggregation");
            result.incapableCause = CapabilityResult.IncapableCause
                    .create(CapabilityResult.IncapableType.LIMIT_PRECEDE_AGGR);
            return result;
        }

        if (digest.isRawQuery && rootFactTable.equals(digest.factTable)) {
            if (cube.getConfig().isDisableCubeNoAggSQL()) {
                result.incapableCause = CapabilityResult.IncapableCause
                        .create(CapabilityResult.IncapableType.UNSUPPORT_RAWQUERY);
                return result;
            } else {
                result.influences.add(new CapabilityInfluence() {
                    @Override
                    public double suggestCostMultiplier() {
                        return 100;
                    }

                    @Override
                    public MeasureDesc getInvolvedMeasure() {
                        return null;
                    }
                });
            }
        }

        // cost will be minded by caller
        result.capable = true;
        return result;
    }

    private static Collection<TblColRef> getDimensionColumns(SQLDigest sqlDigest) {
        Collection<TblColRef> groupByColumns = sqlDigest.groupbyColumns;
        Collection<TblColRef> filterColumns = sqlDigest.filterColumns;

        Collection<TblColRef> dimensionColumns = new HashSet<TblColRef>();
        dimensionColumns.addAll(groupByColumns);
        dimensionColumns.addAll(filterColumns);
        return dimensionColumns;
    }

    private static Set<TblColRef> unmatchedDimensions(Collection<TblColRef> dimensionColumns, CubeInstance cube) {
        HashSet<TblColRef> result = Sets.newHashSet(dimensionColumns);
        CubeDesc cubeDesc = cube.getDescriptor();
        result.removeAll(cubeDesc.listDimensionColumnsIncludingDerived());
        return result;
    }

    private static Set<FunctionDesc> unmatchedAggregations(Collection<FunctionDesc> aggregations, CubeInstance cube) {
        HashSet<FunctionDesc> result = Sets.newHashSet(aggregations);
        CubeDesc cubeDesc = cube.getDescriptor();
        result.removeAll(cubeDesc.listAllFunctions());
        return result;
    }

    static void tryDimensionAsMeasures(Collection<FunctionDesc> unmatchedAggregations, CapabilityResult result,
            Set<TblColRef> dimCols, boolean queryOnFactTable) {

        Iterator<FunctionDesc> it = unmatchedAggregations.iterator();
        while (it.hasNext()) {
            FunctionDesc functionDesc = it.next();

            if (!queryOnFactTable && functionDesc.isCount()) {
                it.remove();
                continue;
            }

            // calcite can do aggregation from columns on-the-fly
            ParameterDesc parameterDesc = functionDesc.getParameter();
            if (parameterDesc == null)
                continue;

            Set<String> buildAggregations = queryOnFactTable ? FunctionDesc.DIMENSION_AS_MEASURES
                    : FunctionDesc.BUILT_IN_AGGREGATIONS;
            List<TblColRef> neededCols = parameterDesc.getColRefs();
            if (neededCols.size() > 0 && dimCols.containsAll(neededCols)) {
                if (FunctionDesc.FUNC_SUM.equals(functionDesc.getExpression()) && parameterDesc.isMathExpressionType())
                    continue;

                if (buildAggregations.contains(functionDesc.getExpression())) {
                    result.influences.add(new CapabilityResult.DimensionAsMeasure(functionDesc));
                    it.remove();
                    continue;
                }
            }
        }
    }

    // custom measure types can cover unmatched dimensions or measures
    private static void tryCustomMeasureTypes(Collection<TblColRef> unmatchedDimensions,
            Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, CubeInstance cube,
            CapabilityResult result) {
        CubeDesc cubeDesc = cube.getDescriptor();
        List<String> influencingMeasures = Lists.newArrayList();
        for (MeasureDesc measure : cubeDesc.getMeasures()) {
            //            if (unmatchedDimensions.isEmpty() && unmatchedAggregations.isEmpty())
            //                break;

            MeasureType<?> measureType = measure.getFunction().getMeasureType();
            if (measureType instanceof BasicMeasureType)
                continue;

            CapabilityInfluence inf = measureType.influenceCapabilityCheck(unmatchedDimensions, unmatchedAggregations,
                    digest, measure);
            if (inf != null) {
                result.influences.add(inf);
                influencingMeasures.add(measure.getName() + "@" + measureType.getClass());
            }
        }
        if (influencingMeasures.size() != 0)
            logger.info("Cube {} CapabilityInfluences: {}", cube.getCanonicalName(),
                    StringUtils.join(influencingMeasures, ","));
    }

    private static void removeUnmatchedGroupingAgg(Collection<FunctionDesc> unmatchedAggregations) {
        if (CollectionUtils.isEmpty(unmatchedAggregations))
            return;

        Iterator<FunctionDesc> iterator = unmatchedAggregations.iterator();
        while (iterator.hasNext()) {
            if (FunctionDesc.FUNC_GROUPING.equalsIgnoreCase(iterator.next().getExpression())) {
                iterator.remove();
            }
        }
    }

}
