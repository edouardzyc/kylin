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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

@SuppressWarnings("serial")
abstract public class SnapshotTable extends RootPersistentEntity implements IReadableTable {
    
    @JsonProperty("tableName")
    protected String tableName;

    @JsonProperty("signature")
    protected IReadableTable.TableSignature signature;

    @JsonProperty("snapshotVersion")
    protected String snapshotVersion;

    protected KylinConfig kylinConfig;

    protected static String NULL_STR;
    {
        try {
            // a special placeholder to indicate a NULL; 0, 9, 127, 255 are a few invisible ASCII characters
            NULL_STR = new String(new byte[] { 0, 9, 127, (byte) 255 }, "ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            // does not happen
        }
    }

    abstract public void init(IReadableTable table, TableDesc tableDesc) throws IOException;

    abstract public TableReader getReader() throws IOException;

    abstract public void writeData(DataOutputStream out) throws IOException;

    abstract public void readData(DataInputStream in) throws IOException;

    abstract public int hashCode();

    abstract public boolean equals(Object o);

    public String getResourcePath() {
        return getResourceDir() + "/" + uuid + ".snapshot";
    }

    public String getResourceDir() {
        if (Strings.isNullOrEmpty(tableName)) {
            return getOldResourceDir();
        } else {
            return ResourceStore.SNAPSHOT_RESOURCE_ROOT + "/" + tableName;
        }
    }

    private String getOldResourceDir() {
        return ResourceStore.SNAPSHOT_RESOURCE_ROOT + "/" + new File(signature.getPath()).getName();
    }

    public TableSignature getSignature() {
        return signature;
    }

    public boolean exists() {
        return true;
    }

    public void setKylinConfig(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }
}
