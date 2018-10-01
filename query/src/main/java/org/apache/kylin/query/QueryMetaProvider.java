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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.querymeta.ColumnMetaWithType;
import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.apache.kylin.query.relnode.OLAPContext;

public class QueryMetaProvider {

    public final static String FakeSchemaName = "defaultSchema";
    public final static String FakeCatalogName = "defaultCatalog";

    @SuppressWarnings("checkstyle:methodlength")
    public static List<TableMetaWithType> getQueryMeta(KylinConfig kylinConfig, String project) throws SQLException {
        //Message msg = MsgPicker.getMsg();

        QueryConnection conn = null;
        ResultSet columnMeta = null;
        List<TableMetaWithType> tableMetas = null;
        Map<String, TableMetaWithType> tableMap = null;
        Map<String, ColumnMetaWithType> columnMap = null;
        ProjectManager prjMgr = ProjectManager.getInstance(kylinConfig);
        DataModelManager dataModelMgr = DataModelManager.getInstance(kylinConfig);

        if (StringUtils.isBlank(project)) {
            return Collections.emptyList();
        }
        ResultSet JDBCTableMeta = null;
        try {
            conn = new QueryConnection(project);
            DatabaseMetaData metaData = conn.getMetaData();

            JDBCTableMeta = metaData.getTables(null, null, null, null);

            tableMetas = new LinkedList<TableMetaWithType>();
            tableMap = new HashMap<String, TableMetaWithType>();
            columnMap = new HashMap<String, ColumnMetaWithType>();
            while (JDBCTableMeta.next()) {
                String catalogName = JDBCTableMeta.getString(1);
                String schemaName = JDBCTableMeta.getString(2);

                // Not every JDBC data provider offers full 10 columns, e.g., PostgreSQL has only 5
                TableMetaWithType tblMeta = new TableMetaWithType(catalogName == null ? FakeCatalogName : catalogName,
                        schemaName == null ? FakeSchemaName : schemaName, JDBCTableMeta.getString(3),
                        JDBCTableMeta.getString(4), JDBCTableMeta.getString(5), null, null, null, null, null);

                if (!"metadata".equalsIgnoreCase(tblMeta.getTABLE_SCHEM())) {
                    tableMetas.add(tblMeta);
                    tableMap.put(tblMeta.getTABLE_SCHEM() + "#" + tblMeta.getTABLE_NAME(), tblMeta);
                }
            }

            columnMeta = metaData.getColumns(null, null, null, null);

            while (columnMeta.next()) {
                String catalogName = columnMeta.getString(1);
                String schemaName = columnMeta.getString(2);

                // kylin(optiq) is not strictly following JDBC specification
                ColumnMetaWithType colmnMeta = new ColumnMetaWithType(
                        catalogName == null ? FakeCatalogName : catalogName,
                        schemaName == null ? FakeSchemaName : schemaName, columnMeta.getString(3),
                        columnMeta.getString(4), columnMeta.getInt(5), columnMeta.getString(6), columnMeta.getInt(7),
                        getInt(columnMeta.getString(8)), columnMeta.getInt(9), columnMeta.getInt(10),
                        columnMeta.getInt(11), columnMeta.getString(12), columnMeta.getString(13),
                        getInt(columnMeta.getString(14)), getInt(columnMeta.getString(15)), columnMeta.getInt(16),
                        columnMeta.getInt(17), columnMeta.getString(18), columnMeta.getString(19),
                        columnMeta.getString(20), columnMeta.getString(21), getShort(columnMeta.getString(22)),
                        columnMeta.getString(23));

                if (!"metadata".equalsIgnoreCase(colmnMeta.getTABLE_SCHEM())
                        && !colmnMeta.getCOLUMN_NAME().toUpperCase().startsWith("_KY_")) {
                    tableMap.get(colmnMeta.getTABLE_SCHEM() + "#" + colmnMeta.getTABLE_NAME()).addColumn(colmnMeta);
                    columnMap.put(colmnMeta.getTABLE_SCHEM() + "#" + colmnMeta.getTABLE_NAME() + "#"
                            + colmnMeta.getCOLUMN_NAME(), colmnMeta);
                }
            }

        } finally {
            close(columnMeta, null, conn);
            if (JDBCTableMeta != null) {
                JDBCTableMeta.close();
            }
        }

        ProjectInstance projectInstance = prjMgr.getProject(project);
        for (String modelName : projectInstance.getModels()) {

            DataModelDesc dataModelDesc = dataModelMgr.getDataModelDesc(modelName);
            if (dataModelDesc != null && !dataModelDesc.isDraft()) {

                // update table type: FACT
                for (TableRef factTable : dataModelDesc.getFactTables()) {
                    String factTableName = factTable.getTableIdentity().replace('.', '#');
                    if (tableMap.containsKey(factTableName)) {
                        tableMap.get(factTableName).getTYPE().add(TableMetaWithType.tableTypeEnum.FACT);
                    } else {
                        // should be used after JDBC exposes all tables and columns
                        // throw new BadRequestException(msg.getTABLE_META_INCONSISTENT());
                    }
                }

                // update table type: LOOKUP
                for (TableRef lookupTable : dataModelDesc.getLookupTables()) {
                    String lookupTableName = lookupTable.getTableIdentity().replace('.', '#');
                    if (tableMap.containsKey(lookupTableName)) {
                        tableMap.get(lookupTableName).getTYPE().add(TableMetaWithType.tableTypeEnum.LOOKUP);
                    } else {
                        // throw new BadRequestException(msg.getTABLE_META_INCONSISTENT());
                    }
                }

                // update column type: PK and FK
                for (JoinTableDesc joinTableDesc : dataModelDesc.getJoinTables()) {
                    JoinDesc joinDesc = joinTableDesc.getJoin();
                    for (String pk : joinDesc.getPrimaryKey()) {
                        String columnIdentity = (dataModelDesc.findTable(pk.substring(0, pk.indexOf(".")))
                                .getTableIdentity() + pk.substring(pk.indexOf("."))).replace('.', '#');
                        if (columnMap.containsKey(columnIdentity)) {
                            columnMap.get(columnIdentity).getTYPE().add(ColumnMetaWithType.columnTypeEnum.PK);
                        } else {
                            // throw new BadRequestException(msg.getCOLUMN_META_INCONSISTENT());
                        }
                    }

                    for (String fk : joinDesc.getForeignKey()) {
                        String columnIdentity = (dataModelDesc.findTable(fk.substring(0, fk.indexOf(".")))
                                .getTableIdentity() + fk.substring(fk.indexOf("."))).replace('.', '#');
                        if (columnMap.containsKey(columnIdentity)) {
                            columnMap.get(columnIdentity).getTYPE().add(ColumnMetaWithType.columnTypeEnum.FK);
                        } else {
                            // throw new BadRequestException(msg.getCOLUMN_META_INCONSISTENT());
                        }
                    }
                }

                // update column type: DIMENSION AND MEASURE
                List<ModelDimensionDesc> dimensions = dataModelDesc.getDimensions();
                for (ModelDimensionDesc dimension : dimensions) {
                    for (String column : dimension.getColumns()) {
                        String columnIdentity = (dataModelDesc.findTable(dimension.getTable()).getTableIdentity() + "."
                                + column).replace('.', '#');
                        if (columnMap.containsKey(columnIdentity)) {
                            columnMap.get(columnIdentity).getTYPE().add(ColumnMetaWithType.columnTypeEnum.DIMENSION);
                        } else {
                            // throw new BadRequestException(msg.getCOLUMN_META_INCONSISTENT());
                        }

                    }
                }

                String[] measures = dataModelDesc.getMetrics();
                for (String measure : measures) {
                    String columnIdentity = (dataModelDesc.findTable(measure.substring(0, measure.indexOf(".")))
                            .getTableIdentity() + measure.substring(measure.indexOf("."))).replace('.', '#');
                    if (columnMap.containsKey(columnIdentity)) {
                        columnMap.get(columnIdentity).getTYPE().add(ColumnMetaWithType.columnTypeEnum.MEASURE);
                    } else {
                        // throw new BadRequestException(msg.getCOLUMN_META_INCONSISTENT());
                    }
                }
            }
        }

        return tableMetas;
    }

    private static int getInt(String content) {
        try {
            return Integer.parseInt(content);
        } catch (Exception e) {
            return -1;
        }
    }

    private static short getShort(String content) {
        try {
            return Short.parseShort(content);
        } catch (Exception e) {
            return -1;
        }
    }

    private static void close(ResultSet resultSet, Statement stat, QueryConnection conn) {
        OLAPContext.clearParameter();
        DBUtils.closeQuietly(resultSet);
        DBUtils.closeQuietly(stat);
        DBUtils.closeQuietly(conn);
    }
}
