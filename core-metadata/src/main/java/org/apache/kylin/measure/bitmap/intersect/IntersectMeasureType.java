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

package org.apache.kylin.measure.bitmap.intersect;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.bitmap.BitmapIntersectDistinctCountAggFunc;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;

import com.google.common.collect.ImmutableMap;

public class IntersectMeasureType extends MeasureType<IntersectBitmapCounter> {
    public static final String FUNC_INTERSECT_COUNT_DISTINCT = FunctionDesc.FUNC_INTERSECT_COUNT_DISTINCT;
    public static final String DATATYPE_BITMAP = "intersect_bitmap";

    public DataType dataType;

    public IntersectMeasureType(String funcName, DataType dataType) {
        this.dataType = dataType;
    }

    public static class Factory extends MeasureTypeFactory<IntersectBitmapCounter> {

        @Override
        public MeasureType<IntersectBitmapCounter> createMeasureType(String funcName, DataType dataType) {
            return new IntersectMeasureType(funcName, dataType);
        }
        // cube chose
        // agg modify
        @Override
        public String getAggrFunctionName() {
            return FUNC_INTERSECT_COUNT_DISTINCT;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_BITMAP;
        }

        @Override
        public Class<? extends DataTypeSerializer<IntersectBitmapCounter>> getAggrDataTypeSerializer() {
            return IntersectSerializer.class;
        }
    }

    @Override
    public List<TblColRef> getColumnsNeedDictionary(FunctionDesc functionDesc) {
        if (needDictionaryColumn(functionDesc)) {
            return Collections.singletonList(functionDesc.getParameter().getColRefs().get(0));
        } else {
            return Collections.emptyList();
        }
    }

    // In order to keep compatibility with old version, tinyint/smallint/int column use value directly, without dictionary
    private boolean needDictionaryColumn(FunctionDesc functionDesc) {
        DataType dataType = functionDesc.getParameter().getColRefs().get(0).getType();
        if (dataType.isIntegerFamily() && !dataType.isBigInt()) {
            return false;
        }
        return true;
    }



    @Override
    public MeasureIngester<IntersectBitmapCounter> newIngester() {
        return null;
    }

    @Override
    public MeasureAggregator<IntersectBitmapCounter> newAggregator() {
        return new IntersectMeasureAggregator();
    }

    private static final Map<String, Class<?>> UDAF_MAP = ImmutableMap
            .<String, Class<?>> of(FUNC_INTERSECT_COUNT_DISTINCT, BitmapIntersectDistinctCountAggFunc.class);

    @Override
    public Map<String, Class<?>> getRewriteCalciteAggrFunctions() {
        return UDAF_MAP;
    }

    @Override
    public boolean needRewrite() {
        return true;
    }

    @Override
    public void adjustSqlDigest(List<MeasureDesc> measureDescs, SQLDigest sqlDigest) {
        for (SQLDigest.SQLCall call : sqlDigest.aggrSqlCalls) {
            if (FUNC_INTERSECT_COUNT_DISTINCT.equals(call.function)) {
                TblColRef col = (TblColRef) call.args.get(1);
                if (!sqlDigest.groupbyColumns.contains(col))
                    sqlDigest.groupbyColumns.add(col);
            }
        }
    }
}
