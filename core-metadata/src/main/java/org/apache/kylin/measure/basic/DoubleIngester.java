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

package org.apache.kylin.measure.basic;

import java.util.Map;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;

@SuppressWarnings("serial")
public class DoubleIngester extends MeasureIngester<Double> {

    @Override
    public Double valueOf(String[] values, MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dictionaryMap) {
        ParameterDesc param = measureDesc.getFunction().getParameter();
        if (FunctionDesc.PARAMETER_TYPE_MATH_EXPRESSION.equals(param.getType())) {
            return param.getExpressionParam().getValueOf(values).doubleValue();
        }

        if (values.length > 1)
            throw new IllegalArgumentException();

        if (values[0] == null || values[0].length() == 0) {
            return null;
        } else {
            return Double.parseDouble(values[0]);
        }
    }

    @Override
    public void reset() {

    }
}
