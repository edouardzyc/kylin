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

package org.apache.kylin.rest.service;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.rest.msg.MsgPicker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TableSchemaUpdateChecker {
    private final TableMetadataManager metadataManager;
    private final DataModelManager modelManager;
    private final CubeManager cubeManager;

    public static class CheckResultEntity {
        public final String fullTableName;
        public final boolean isLookupTblBroken;
        public final List<String> brokenCols;
        public final List<String> brokenModels;
        public final List<String> brokenCubes;

        public CheckResultEntity(String fullTableName, boolean isLookupTblBroken, List<String> brokenCols,
                List<String> brokenModels, List<String> brokenCubes) {
            this.fullTableName = fullTableName;
            this.isLookupTblBroken = isLookupTblBroken;
            this.brokenCols = brokenCols;
            this.brokenModels = brokenModels;
            this.brokenCubes = brokenCubes;
        }

        public CheckResult getCheckResult() {
            if (!isLookupTblBroken && brokenCols.size() == 0 && brokenModels.size() == 0 && brokenCubes.size() == 0)
                return CheckResult.validOnCompatibleSchema(fullTableName);

            return CheckResult.invalidOnIncompatibleSchema(fullTableName, isLookupTblBroken, brokenCols, brokenModels,
                    brokenCubes);
        }
    }

    public static class CheckResult {
        private static final Logger logger = LoggerFactory.getLogger(CheckResult.class);

        private final boolean valid;
        private final String reason;

        private CheckResult(boolean valid, String reason) {
            this.valid = valid;
            this.reason = reason;
        }

        public boolean isValid() {
            return valid;
        }

        public String getReason() {
            return reason;
        }

        void raiseExceptionWhenInvalid() {
            if (!valid) {
                throw new RuntimeException(reason);
            }
        }

        public static CheckResult validOnFirstLoad(String tableName) {
            return new CheckResult(true, format("Table '%s' hasn't been loaded before", tableName));
        }

        static CheckResult validOnCompatibleSchema(String tableName) {
            return new CheckResult(true, format("Table '%s' is compatible with all existing cubes", tableName));
        }

        static CheckResult invalidOnFetchSchema(String tableName, Exception e) {
            return new CheckResult(false, format("Failed to fetch metadata of '%s': %s", tableName, e.getMessage()));
        }

        static CheckResult invalidOnIncompatibleSchema(String tableName, boolean isLookupTblChanged,
                List<String> brokenCols, List<String> brokenModels, List<String> brokenCubes) {

            StringBuilder logBuf = new StringBuilder();
            String msg = MsgPicker.getMsg().getLOAD_HIVE_TABLES_FAILED(isLookupTblChanged, brokenCols, brokenModels);

            if (isLookupTblChanged) {
                logBuf.append(format("Table %s is used as Lookup Table but changed in hive , ", tableName));
            }
            if (brokenCols.size() > 0) {
                logBuf.append(format("Table %s has %d Column(s) changed : %s , ", tableName, brokenCols.size(),
                        brokenCols.toString()));
            }
            if (brokenModels.size() > 0) {
                logBuf.append(format("influence %d Model(s) : %s , ", brokenModels.size(), brokenModels.toString()));
            }
            if (brokenCubes.size() > 0) {
                logBuf.append(format("influence %d Cube(s) : %s , ", brokenCubes.size(), brokenCubes.toString()));
            }
            logBuf.append("Please purge and delete related Cube(s) , then modify related Model(s)");

            logger.error(logBuf.toString(), new RuntimeException());
            return new CheckResult(false, msg);
        }
    }

    public TableSchemaUpdateChecker(TableMetadataManager metadataManager, DataModelManager modelManager,
            CubeManager cubeManager) {
        this.metadataManager = checkNotNull(metadataManager, "metadataManager is null");
        this.modelManager = checkNotNull(modelManager, "modelManager is null");
        this.cubeManager = checkNotNull(cubeManager, "cubeManager is null");
    }

    private List<CubeInstance> findCubesByTable(final TableDesc table) {
        Iterable<CubeInstance> relatedCubes = Iterables.filter(cubeManager.listAllCubes(),
                new Predicate<CubeInstance>() {
                    @Override
                    public boolean apply(@Nullable CubeInstance cube) {
                        if (cube == null || cube.isDescBroken())
                            return false;

                        DataModelDesc model = cube.getModel();
                        if (model == null)
                            return false;
                        return model.containsTable(table);
                    }
                });

        return ImmutableList.copyOf(relatedCubes);
    }

    private List<DataModelDesc> findModelsByTable(final TableDesc table, final String project) {
        Iterable<DataModelDesc> relatedModels = Iterables.filter(modelManager.getModels(project),
                new Predicate<DataModelDesc>() {
                    @Override
                    public boolean apply(@Nullable DataModelDesc model) {
                        if (model == null || model.isBroken())
                            return false;
                        return model.containsTable(table);
                    }
                });
        return ImmutableList.copyOf(relatedModels);
    }

    public List<String> checkNotExistedAndPartitionColTypeChangedColsInModel(DataModelDesc model, TableDesc origTable,
            TableDesc newTable) {
        List<String> violateColumnsFromModel = Lists.newArrayList();

        //get columns used by model to check existed
        Set<String> usedColumns = model.getAllDimsAndMetricsWithTable(origTable.getName());
        //get partition column to check existed and type changed
        String partitionTblCol = model.getPartitionDesc().getPartitionDateColumn();
        String partitionColName = partitionTblCol == null ? ""
                : partitionTblCol.substring(partitionTblCol.lastIndexOf(".") + 1);

        for (ColumnDesc origColumn : origTable.getColumns()) {
            //check if used origColumn exists
            if (!origColumn.isComputedColumn() && usedColumns.contains(origColumn.getName())) {
                ColumnDesc newCol = newTable.findColumnByName(origColumn.getName());
                if (newCol == null)
                    violateColumnsFromModel.add(origColumn.getName());
            }
            //check if partition origColumn exists and type not changed
            if (origColumn.getName().equals(partitionColName)
                    && !violateColumnsFromModel.contains(origColumn.getName())) {
                ColumnDesc newCol = newTable.findColumnByName(origColumn.getName());
                if (newCol == null || !newCol.isColumnCompatible(origColumn))
                    violateColumnsFromModel.add(origColumn.getName());
            }
        }
        return violateColumnsFromModel;
    }

    /**
     * check whether all columns used in `cube` has compatible schema in current hive schema denoted by `fieldsMap`.
     * @param cube cube to check, must use `table` in its model
     * @param origTable kylin's table metadata
     * @param newTable current hive schema of `table`
     * @return true if all columns used in `cube` has compatible schema with `fieldsMap`, false otherwise
     */
    public List<String> checkTypeChangedAndNotExistedColsInCube(CubeInstance cube, TableDesc origTable,
            TableDesc newTable) {
        Set<ColumnDesc> usedColumns = Sets.newHashSet();
        for (TblColRef col : cube.getAllColumns()) {
            usedColumns.add(col.getColumnDesc());
        }

        List<String> violateColumns = Lists.newArrayList();
        for (ColumnDesc origColumn : origTable.getColumns()) {
            if (!origColumn.isComputedColumn() && usedColumns.contains(origColumn)) {
                ColumnDesc newCol = newTable.findColumnByName(origColumn.getName());
                //check if origColumn exists and check if origColumn type changed
                if (newCol == null || !newCol.isColumnCompatible(origColumn)) {
                    violateColumns.add(origColumn.getName());
                }
            }
        }
        return violateColumns;
    }

    /**
     * check whether all columns in `table` are still in `fields` and have the same index as before.
     *
     * @param origTable kylin's table metadata
     * @param newTable current table metadata in hive
     * @return true if only new columns are appended in hive, false otherwise
     */
    public boolean checkAllColumnsInTableDesc(TableDesc origTable, TableDesc newTable) {
        if (origTable.getColumnCount() > newTable.getColumnCount()) {
            return false;
        }

        ColumnDesc[] columns = origTable.getColumns();
        for (int i = 0; i < columns.length; i++) {
            if (!newTable.getColumns()[i].isColumnCompatible(columns[i])) {
                return false;
            }
        }
        return true;
    }

    public CheckResultEntity checkAllowReload(String fullTableName, TableDesc newTableDesc, String prj) {
        boolean isLookupTblBroken = false;
        final List<String> brokenCols = new LinkedList<>();
        final List<String> brokenModels = new LinkedList<>();
        final List<String> brokenCubes = new LinkedList<>();

        //check models
        List<DataModelDesc> models = findModelsByTable(newTableDesc, prj);
        for (DataModelDesc model : models) {
            // if user reloads a table used by model, then all used columns must exist in current schema, and partition column type can not change
            TableDesc table = model.findFirstTable(fullTableName).getTableDesc();
            List<String> violateColumnsFromModel = checkNotExistedAndPartitionColTypeChangedColsInModel(model, table,
                    newTableDesc);
            if (!violateColumnsFromModel.isEmpty()) {
                for (String col : violateColumnsFromModel) {
                    if (!brokenCols.contains(col)) {
                        brokenCols.add(col);
                    }
                }
                if (!brokenModels.contains(model.getName())) {
                    brokenModels.add(model.getName());
                }
            }
        }

        //check cubes
        for (CubeInstance cube : findCubesByTable(newTableDesc)) {
            DataModelDesc model = cube.getModel();
            // if user reloads a table used by cube, then all used columns must exist and type can not change
            TableDesc table = cube.getModel().findFirstTable(fullTableName).getTableDesc();
            List<String> violateColumnsFromCube = checkTypeChangedAndNotExistedColsInCube(cube, table, newTableDesc);
            if (!violateColumnsFromCube.isEmpty()) {
                for (String col : violateColumnsFromCube) {
                    if (!brokenCols.contains(col)) {
                        brokenCols.add(col);
                    }
                }
                if (!brokenModels.contains(model.getName())) {
                    brokenModels.add(model.getName());
                }
                if (!brokenCubes.contains(cube.getName())) {
                    brokenCubes.add(cube.getName());
                }
            }

            //if table is lookup table,and the segment of the cube that used this table is not empty,but changed in hive ,will influence snapshot
            if (model.isLookupTable(fullTableName) && !cube.getSegments().isEmpty()
                    && !checkAllColumnsInTableDesc(table, newTableDesc)) {
                isLookupTblBroken = true;
                if (!brokenCubes.contains(cube.getName())) {
                    brokenCubes.add(cube.getName());
                }
            }
        }
        return new CheckResultEntity(fullTableName, isLookupTblBroken, brokenCols, brokenModels, brokenCubes);
    }

    public CheckResult allowReload(TableDesc newTableDesc, String prj) {
        final String fullTableName = newTableDesc.getIdentity();
        TableDesc origTable = metadataManager.getTableDesc(fullTableName, prj);
        if (origTable == null) {
            return CheckResult.validOnFirstLoad(fullTableName);
        }
        CheckResultEntity checkResultEntity = checkAllowReload(fullTableName, newTableDesc, prj);
        return checkResultEntity.getCheckResult();
    }
}
