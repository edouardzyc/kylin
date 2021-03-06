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

package org.apache.kylin.source.hive;

import java.io.Closeable;
import java.util.List;

public interface IHiveClient extends Closeable {

    void executeHQL(String hql) throws Exception;

    void executeHQL(String[] hqls) throws Exception;
    
    HiveTableMeta getHiveTableMeta(String database, String tableName) throws Exception;

    List<String> getHiveDbNames() throws Exception;

    List<String> getHiveTableNames(String database) throws Exception;

    long getHiveTableRows(String database, String tableName) throws Exception;

    // IHiveClient must be closed
    void close();
}
