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

import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.bitmap.BitmapCounter;
import org.apache.kylin.measure.bitmap.BitmapCounterFactory;
import org.apache.kylin.measure.bitmap.RoaringBitmapCounterFactory;

/**
 *  This is dummy aggregator, the effective aggregation is done in org.apache.spark.sql.udf.SparderExternalAggFunc
 */
public class IntersectMeasureAggregator extends MeasureAggregator<IntersectBitmapCounter> {
    private static final BitmapCounterFactory bitmapFactory = RoaringBitmapCounterFactory.INSTANCE;

    private IntersectBitmapCounter sum;

    @Override
    public void reset() {
        sum = null;
    }

    @Override
    public void aggregate(IntersectBitmapCounter value) {
    }

    @Override
    public IntersectBitmapCounter aggregate(IntersectBitmapCounter value1, IntersectBitmapCounter value2) {



        return value1;
    }

    @Override
    public IntersectBitmapCounter getState() {
        return sum;
    }

    @Override
    public int getMemBytesEstimate() {
        return sum == null ? 0 : ((BitmapCounter) sum).getMemBytes();
    }

}
