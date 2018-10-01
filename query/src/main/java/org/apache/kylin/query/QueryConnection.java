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

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.jdbc.Driver;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.query.schema.OLAPSchemaFactory;

import com.google.common.collect.Maps;

/**
 * The single entry point to Kylin query function, for both QueryService and tests.
 * 
 * - Encapsulates (and hides) calcite connection mostly because of the special PreparedStatement behavior.
 */
public class QueryConnection implements AutoCloseable {

    private static boolean isRegister = false;

    final private Connection conn;

    public QueryConnection(String project) throws SQLException {
        registerDriverIfNeeded();

        File olapTmp = OLAPSchemaFactory.getOrCreateTempOLAPJson(project, KylinConfig.getInstanceFromEnv());
        Properties info = new Properties();
        info.put("model", olapTmp.getAbsolutePath());
        info.put("typeSystem", "org.apache.kylin.query.calcite.KylinRelDataTypeSystem");
        info.put("DEFAULT_NULL_COLLATION", "LOW");
        conn = DriverManager.getConnection("jdbc:calcite:", info);
    }

    private void registerDriverIfNeeded() throws SQLException {
        if (!isRegister) {
            try {
                Class<?> aClass = Thread.currentThread().getContextClassLoader()
                        .loadClass("org.apache.calcite.jdbc.Driver");
                Driver o = (Driver) aClass.newInstance();
                DriverManager.registerDriver(o);
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
            isRegister = true;
        }
    }

    public String getSchema() throws SQLException {
        return conn.getSchema();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return conn.getMetaData();
    }

    // for normal non-prepared statement
    public Statement createStatement() throws SQLException {
        return conn.createStatement();
    }

    // for prepared statement
    public PreparedStatement prepareStatement(String sql, Object[] params) throws SQLException {
        StateParam[] sp = new StateParam[params.length];
        for (int i = 0; i < params.length; i++) {
            sp[i] = new StateParam(params[i]);
        }

        return prepareStatement(sql, sp);
    }

    // for prepared statement
    public PreparedStatement prepareStatement(String sql, StateParam[] params) throws SQLException {

        // OLAPToEnumerableConverter needs the parameters in context, in order to select the right MPCube
        Map<String, Object> contextParams = Maps.newHashMap();
        for (int i = 0; i < params.length; i++) {
            Object o = setParam(null, i + 1, params[i]);
            contextParams.put("?" + i, o);
        }
        QueryContext.current().setPrepareParams(contextParams);

        // here goes into OLAPToEnumerableConverter
        PreparedStatement stat = conn.prepareStatement(sql);

        // set the parameters on the returning PreparedStatement
        for (int i = 0; i < params.length; i++) {
            setParam(stat, i + 1, params[i]);
        }
        return stat;
    }

    private Object setParam(PreparedStatement stat, int index, StateParam param) throws SQLException {
        boolean isNull = (null == param.getValue());

        Class<?> clazz;
        try {
            clazz = Class.forName(param.getClassName());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }

        Object o = null;
        Rep rep = Rep.of(clazz);

        switch (rep) {
        case PRIMITIVE_CHAR:
        case CHARACTER:
        case STRING:
            o = isNull ? null : String.valueOf(param.getValue());
            if (stat != null)
                stat.setString(index, (String) o);
            break;
        case PRIMITIVE_INT:
        case INTEGER:
            o = isNull ? 0 : Integer.valueOf(param.getValue());
            if (stat != null)
                stat.setInt(index, (Integer) o);
            break;
        case PRIMITIVE_SHORT:
        case SHORT:
            o = isNull ? 0 : Short.valueOf(param.getValue());
            if (stat != null)
                stat.setShort(index, (Short) o);
            break;
        case PRIMITIVE_LONG:
        case LONG:
            o = isNull ? 0 : Long.valueOf(param.getValue());
            if (stat != null)
                stat.setLong(index, (Long) o);
            break;
        case PRIMITIVE_FLOAT:
        case FLOAT:
            o = isNull ? 0 : Float.valueOf(param.getValue());
            if (stat != null)
                stat.setFloat(index, (Float) o);
            break;
        case PRIMITIVE_DOUBLE:
        case DOUBLE:
            o = isNull ? 0 : Double.valueOf(param.getValue());
            if (stat != null)
                stat.setDouble(index, (Double) o);
            break;
        case PRIMITIVE_BOOLEAN:
        case BOOLEAN:
            o = !isNull && Boolean.parseBoolean(param.getValue());
            if (stat != null)
                stat.setBoolean(index, (Boolean) o);
            break;
        case PRIMITIVE_BYTE:
        case BYTE:
            o = isNull ? 0 : Byte.valueOf(param.getValue());
            if (stat != null)
                stat.setByte(index, (Byte) o);
            break;
        case JAVA_UTIL_DATE:
        case JAVA_SQL_DATE:
            o = isNull ? null : java.sql.Date.valueOf(param.getValue());
            if (stat != null)
                stat.setDate(index, (java.sql.Date) o);
            break;
        case JAVA_SQL_TIME:
            o = isNull ? null : Time.valueOf(param.getValue());
            if (stat != null)
                stat.setTime(index, (Time) o);
            break;
        case JAVA_SQL_TIMESTAMP:
            o = isNull ? null : Timestamp.valueOf(param.getValue());
            if (stat != null)
                stat.setTimestamp(index, (Timestamp) o);
            break;
        default:
            o = isNull ? null : param.getObjectValue();
            if (stat != null)
                stat.setObject(index, o);
        }

        return o;
    }

    @Override
    public void close() throws SQLException {

        // ideally, we could reset QueryContext & BackdoorToggles here as well, however the refactoring is tough...

        // close calcite connection
        conn.close();
    }

    public static class StateParam {
        private String className;
        private String value;
        private Object objectValue;

        public StateParam() {
        }

        public StateParam(Object object) {
            setObjectValue(object);
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public Object getObjectValue() {
            return objectValue;
        }

        public void setObjectValue(Object object) {
            objectValue = object;

            if (object != null) {
                className = object.getClass().getName();
                value = object.toString();
            }
        }
    }

}
