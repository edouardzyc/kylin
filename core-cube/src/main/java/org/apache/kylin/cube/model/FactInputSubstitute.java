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

package org.apache.kylin.cube.model;

import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.PartitionDesc.IPartitionConditionBuilder;

import java.io.IOException;

/**
 * Implementing this abstract class, subclass can substitute the fact table during build process,
 * loading data from any specific table rather than the original fact table defined in model.
 */
public abstract class FactInputSubstitute {

    public static FactInputSubstitute getInstance(ISegment segment) {
        if (segment == null)
            return null;
        if (!(segment instanceof CubeSegment))
            return null;
        
        CubeSegment seg = (CubeSegment) segment;
        String clsName = seg.getConfig().getFactInputSubstitute();
        if (clsName == null || clsName.isEmpty())
            return null;

        try {
            return ClassUtil.forName(clsName, FactInputSubstitute.class)//
                    .getConstructor(CubeSegment.class).newInstance(seg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ============================================================================

    final protected CubeSegment seg;

    public FactInputSubstitute(CubeSegment seg) {
        this.seg = seg;
    }

    abstract public void setupFactTableBeforeBuild() throws IOException;

    abstract public void tearDownFactTableAfterBuild() throws IOException;

    public String getFactTableSubstitute() {
        String database = seg.getConfig().getHiveDatabaseForIntermediateTable();
        String table = CubeJoinedFlatTableDesc.makeIntermediateTableName(seg.getCubeDesc(), seg) + "_fact";
        return database + "." + table;
    }

    public String getRootFactTable() {
        return seg.getCubeDesc().getModel().getRootFactTableName();
    }

    public IPartitionConditionBuilder getPartitionConditionBuilder() {
        return null; // return null means full table
    }
}
