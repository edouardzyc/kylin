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

package org.apache.kylin.storage.LocalFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.FileResourceStore;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.persistence.WriteConflictException;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ITFileResourceStoreTest extends LocalFileMetadataTestCase {
    private FileResourceStore store;
    private String path = "/table_snapshot/_test_put_resource_streaming.json";
    private String tmpPath = path + ".tmp";
    private String contentStr = "THIS_IS_PUT_RESOURCE_STREAMING";
    private StringEntity content = new StringEntity(contentStr);
    private File file;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        store = new FileResourceStore(KylinConfig.getInstanceFromEnv());
        file = new File(store.resourcePath(path).toString());
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
        store.deleteResource(path);
    }

    @Test
    public void testPutResourceStreaming() throws IOException {
        store.putResourceStreaming(path, content, StringEntity.serializer);
        assertTrue(store.exists(path));

        StringEntity t = store.getResource(path, StringEntity.class, StringEntity.serializer);
        assertEquals(content, t);
        assertTrue(file.exists());

        FileInputStream in = FileUtils.openInputStream(file);
        DataInputStream dataInputStream = new DataInputStream(in);
        assertEquals(contentStr, dataInputStream.readUTF());
        dataInputStream.close();
    }

    @Test
    public void testWithWriteConflictException() throws IOException {
        store.putResourceStreaming(path, content, StringEntity.serializer);
        assertTrue(store.exists(path));
        FileInputStream fileInputStream = FileUtils.openInputStream(file);
        StringEntity exception = new StringEntity("THIS_IS_EXCEPTION");
        try {
            store.putResourceStreaming(path, exception, StringEntity.serializer);
        } catch (Exception e) {
            assertTrue(e instanceof WriteConflictException);
            fileInputStream.close();
        }

        StringEntity t = store.getResource(path, StringEntity.class, StringEntity.serializer);
        assertEquals(content, t);
        assertFalse(new File(store.resourcePath(tmpPath).toString()).exists());
        fileInputStream = FileUtils.openInputStream(file);
        DataInputStream dataInputStream = new DataInputStream(fileInputStream);
        assertEquals(contentStr, dataInputStream.readUTF());
        dataInputStream.close();
    }
}
