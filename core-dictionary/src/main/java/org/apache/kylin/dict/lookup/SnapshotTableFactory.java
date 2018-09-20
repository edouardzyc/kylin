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

package org.apache.kylin.dict.lookup;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.source.IReadableTable;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class SnapshotTableFactory {
    public static final SnapshotTableFactory INSTANCE = new SnapshotTableFactory();
    private static final Logger logger = LoggerFactory.getLogger(SnapshotTable.class);

    public static Map<String, Class<? extends SnapshotTable>> versionMap = Maps.newHashMap();

    static {
        versionMap.put(SnapshotTableV1.VERSION_ID, SnapshotTableV1.class);
        versionMap.put(SnapshotTableV2.VERSION_ID, SnapshotTableV2.class);
    }

    private SnapshotTableFactory() {
    }

    public static Class<? extends SnapshotTable> getClass(String json) throws IOException {
        try {
            JSONObject object = new JSONObject(json);
            if (object.has("snapshotVersion")) {
                String version = (String) object.get("snapshotVersion");
                return versionMap.get(version);
            } else {
                return versionMap.get(SnapshotTableV1.VERSION_ID);
            }
        } catch (JSONException e) {
            throw new IOException("Wrong in Parse json.", e);
        }
    }

    public static SnapshotTable create(IReadableTable table, String tableName, KylinConfig kylinConfig)
            throws IOException {
        try {
            Class clazz = Class.forName(kylinConfig.getSnapshotTableClass());
            Constructor constructor = clazz.getConstructor(IReadableTable.class, String.class);
            SnapshotTable snapshotTable = (SnapshotTable) constructor.newInstance(table, tableName);
            snapshotTable.setKylinConfig(kylinConfig);
            return snapshotTable;
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException
                | InvocationTargetException e) {
            logger.error("Wrong Snapshot Version" + kylinConfig.getSnapshotTableClass(), e);
            throw new RuntimeException(e);
        }
    }

}
