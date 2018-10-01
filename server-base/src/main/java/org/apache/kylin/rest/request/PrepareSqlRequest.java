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

package org.apache.kylin.rest.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.kylin.query.QueryConnection.StateParam;

/**
 * @author xduo
 */
@SuppressWarnings("serial")
public class PrepareSqlRequest extends SQLRequest {

    public PrepareSqlRequest() {
        super();
    }

    private StateParam[] params;

    public StateParam[] getParams() {
        return params;
    }

    public void setParams(StateParam[] params) {
        this.params = params;
    }

    @Override
    public Object getCacheKey() {
        if (cacheKey != null)
            return cacheKey;

        cacheKey = super.getCacheKey();

        if (params != null) {
            ArrayList keyList = (ArrayList) (cacheKey);
            Collections.addAll(keyList, params);
        }
        return cacheKey;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((params == null) ? 0 : Arrays.hashCode(params));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (!super.equals(obj))
            return false;
        PrepareSqlRequest other = (PrepareSqlRequest) obj;
        if (!Arrays.equals(params, other.params))
            return false;
        return true;
    }
}
