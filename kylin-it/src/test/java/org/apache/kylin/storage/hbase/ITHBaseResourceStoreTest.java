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

package org.apache.kylin.storage.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceStoreTest;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.persistence.WriteConflictException;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.dao.ExecutableOutoutPOSerializer;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ITHBaseResourceStoreTest extends HBaseMetadataTestCase {

    private KylinConfig kylinConfig;
    private String path = "/table_snapshot/_test_put_resource_streaming.json";
    private String tmpPath = path + ".tmp";
    private String contentStr = "THIS_IS_PUT_RESOURCE_STREAMING";
    private StringEntity content = new StringEntity(contentStr);
    private ResourceStore store;
    private String exceptionStr = "EXCEPTION";
    private StringEntity exception = new StringEntity(exceptionStr);
    private Path realPath;
    private FileSystem fileSystem;
    private Configuration originConfiguration;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        kylinConfig = KylinConfig.getInstanceFromEnv();
        store = ResourceStore.getStore(kylinConfig);
        realPath = store.resourcePath(path);
        fileSystem = HadoopUtil.getFileSystem(realPath);
        originConfiguration = HBaseConnection.getCurrentHBaseConfiguration();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
        store.deleteResource(path);
        content.setLastModified(0);
        HadoopUtil.setCurrentConfiguration(new Configuration());
        HBaseConnection.setHBaseConfiguration(originConfiguration);
    }

    @Test
    public void testHBaseStore() throws Exception {
        String storeName = "org.apache.kylin.storage.hbase.HBaseResourceStore";
        ResourceStoreTest.testAStore(ResourceStoreTest.mockUrl("hbase", kylinConfig), kylinConfig);
    }

    @Test
    public void testGetResourceImpl() throws Exception {
        ExecutableOutoutPOSerializer executableOutputPOSerializer = new ExecutableOutoutPOSerializer();
        String uuid = UUID.randomUUID().toString();
        String path = ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + uuid;
        String largeContent = "THIS_IS_A_LARGE_CELL";
        StringEntity largeEntity = new StringEntity(largeContent);
        String oldUrl = ResourceStoreTest.replaceMetadataUrl(kylinConfig,
                ResourceStoreTest.mockUrl("hbase", kylinConfig));
        HBaseResourceStore store = new HBaseResourceStore(KylinConfig.getInstanceFromEnv());
        Configuration hconf = store.getConnection().getConfiguration();
        int origSize = Integer.parseInt(hconf.get("hbase.client.keyvalue.maxsize", "10485760"));

        try {
            hconf.set("hbase.client.keyvalue.maxsize", String.valueOf(largeContent.length() - 1));

            store.deleteResource(path);

            store.putResource(path, largeEntity, StringEntity.serializer);

            Path redirectPath = ((HBaseResourceStore) store).bigCellHDFSPath(path);
            FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
            fileSystem.delete(redirectPath, true);

            try {
                RawResource resource1 = store.getResourceImpl(path, false);
                fail("Expected a IOException to be thrown");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof IOException);
            }

            RawResource resource2 = store.getResourceImpl(path, true);
            ExecutableOutputPO brokenOutput = executableOutputPOSerializer
                    .deserialize(new DataInputStream(resource2.inputStream));
            Assert.assertEquals(uuid, brokenOutput.getUuid());

            ResourceStoreTest.replaceMetadataUrl(kylinConfig, oldUrl);
        } finally {
            hconf.set("hbase.client.keyvalue.maxsize", "" + origSize);
            store.deleteResource(path);
        }
    }

    @Test
    public void testGetAllResourcesImpl() throws Exception {
        String path = ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + UUID.randomUUID().toString();
        String largeContent = "THIS_IS_A_LARGE_CELL";
        String samllContent = "SMALL_CELL";
        StringEntity largeEntity = new StringEntity(largeContent);
        StringEntity smallEntity1 = new StringEntity(samllContent);
        StringEntity smallEntity2 = new StringEntity(samllContent);

        String oldUrl = ResourceStoreTest.replaceMetadataUrl(kylinConfig,
                ResourceStoreTest.mockUrl("hbase", kylinConfig));
        HBaseResourceStore store = new HBaseResourceStore(KylinConfig.getInstanceFromEnv());
        Configuration hconf = store.getConnection().getConfiguration();
        int origSize = Integer.parseInt(hconf.get("hbase.client.keyvalue.maxsize", "10485760"));

        try {
            hconf.set("hbase.client.keyvalue.maxsize", String.valueOf(largeContent.length() - 1));

            store.deleteResource(path);
            store.deleteResource(path + "00");
            store.deleteResource(path + "01");

            store.putResource(path, smallEntity1, StringEntity.serializer);
            store.putResource(path + "-00", largeEntity, StringEntity.serializer);
            store.putResource(path + "-01", smallEntity2, StringEntity.serializer);

            Path redirectPath = ((HBaseResourceStore) store).bigCellHDFSPath(path + "-00");
            FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
            fileSystem.delete(redirectPath, true);

            try {
                List<RawResource> resources1 = store.getAllResourcesImpl(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT,
                        Long.MIN_VALUE, Long.MAX_VALUE, false);
                fail("Expected a IOException to be thrown");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof IOException);
            }

            List<RawResource> resources2 = store.getAllResourcesImpl(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT,
                    Long.MIN_VALUE, Long.MAX_VALUE, true);

            ResourceStoreTest.replaceMetadataUrl(kylinConfig, oldUrl);
        } finally {
            hconf.set("hbase.client.keyvalue.maxsize", "" + origSize);
            store.deleteResource(path);
            store.deleteResource(path + "00");
            store.deleteResource(path + "01");
        }
    }

    @Test
    public void testHBaseStoreWithLargeCell() throws Exception {
        String path = "/cube/_test_large_cell.json";
        String largeContent = "THIS_IS_A_LARGE_CELL";
        StringEntity content = new StringEntity(largeContent);
        String oldUrl = ResourceStoreTest.replaceMetadataUrl(kylinConfig,
                ResourceStoreTest.mockUrl("hbase", kylinConfig));
        HBaseResourceStore store = new HBaseResourceStore(KylinConfig.getInstanceFromEnv());
        Configuration hconf = store.getConnection().getConfiguration();
        int origSize = Integer.parseInt(hconf.get("hbase.client.keyvalue.maxsize", "10485760"));

        try {
            hconf.set("hbase.client.keyvalue.maxsize", String.valueOf(largeContent.length() - 1));

            store.deleteResource(path);

            store.putResource(path, content, StringEntity.serializer);
            assertTrue(store.exists(path));
            StringEntity t = store.getResource(path, StringEntity.class, StringEntity.serializer);
            assertEquals(content, t);

            Path redirectPath = ((HBaseResourceStore) store).bigCellHDFSPath(path);

            FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
            assertTrue(fileSystem.exists(redirectPath));

            FSDataInputStream in = fileSystem.open(redirectPath);
            assertEquals(largeContent, in.readUTF());
            in.close();

            store.deleteResource(path);
            ResourceStoreTest.replaceMetadataUrl(kylinConfig, oldUrl);
        } finally {
            hconf.set("hbase.client.keyvalue.maxsize", "" + origSize);
            store.deleteResource(path);
        }
    }

    @Test
    public void testPutResourceStreaming() throws IOException {
        store.putResourceStreaming(path, content, StringEntity.serializer);
        assertTrue(store.exists(path));

        StringEntity t = store.getResource(path, StringEntity.class, StringEntity.serializer);
        assertEquals(content, t);

        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        assertTrue(fileSystem.exists(realPath));

        FSDataInputStream in = fileSystem.open(realPath);
        assertEquals(contentStr, in.readUTF());
        in.close();
    }

    @Test
    public void testWithWriteConflictException() throws IOException {
        store.putResourceStreaming(path, content, StringEntity.serializer);
        assertTrue(store.exists(path));

        FSDataInputStream in = fileSystem.open(realPath);
        StringEntity exception = new StringEntity("THIS_IS_EXCEPTION");
        try {
            store.putResourceStreaming(path, exception, StringEntity.serializer);
        } catch (Exception e) {
            assertTrue(e instanceof WriteConflictException);
            in.close();
        }

        StringEntity t = store.getResource(path, StringEntity.class, StringEntity.serializer);
        assertEquals(content, t);
        assertFalse(fileSystem.exists(store.resourcePath(tmpPath)));
        in = fileSystem.open(realPath);
        assertEquals(contentStr, in.readUTF());
        in.close();
    }

    @Test
    public void testWithException() throws IOException {
        runExceptionCase("org.apache.kylin.storage.DistributedExceptionFileSystem.RenameExceptionFileSystem",
                "TEST RENAME EXCEPTION");
    }

    private void runExceptionCase(String className, String expectedMessage) throws IOException {
        // write a resource
        long ts = store.putResourceStreaming(path, content, StringEntity.serializer);

        // use exception file system
        KylinConfig.destroyInstance();
        KylinConfig exceptionConfig = KylinConfig.getInstanceFromEnv();
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", className);
        conf.set("fs.hdfs.impl.disable.cache", "true");

        HBaseConnection.setHBaseConfiguration(conf);
        ResourceStore exceptionStore = ResourceStore.getStore(exceptionConfig);

        ResourceStore.Checkpoint cp = exceptionStore.checkpoint();
        try {
            exception.setLastModified(ts);
            exceptionStore.putResourceStreaming(path, exception, StringEntity.serializer);
        } catch (IOException e) {
            HBaseConnection.setHBaseConfiguration(originConfiguration);
            HadoopUtil.setCurrentConfiguration(new Configuration());
            cp.rollback();
            assertFalse(HadoopUtil.getWorkingFileSystem().exists(exceptionStore.resourcePath(tmpPath)));
            assertEquals(expectedMessage, e.getMessage());
            exception.setLastModified(0);
        }

        // check rollback success
        StringEntity t = store.getResource(path, StringEntity.class, StringEntity.serializer);
        assertEquals(content, t);
    }

}
