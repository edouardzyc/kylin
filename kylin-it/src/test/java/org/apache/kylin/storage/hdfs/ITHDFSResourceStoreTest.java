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

package org.apache.kylin.storage.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.ResourceStoreTest;
import org.apache.kylin.common.persistence.StringEntity;
import org.apache.kylin.common.persistence.WriteConflictException;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.HadoopUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ITHDFSResourceStoreTest extends HBaseMetadataTestCase {

    private KylinConfig kylinConfig;
    private ResourceStore store;
    private String path = "/table_snapshot/_test_put_resource_streaming.json";
    private String tmpPath = path + ".tmp";
    private String contentStr = "THIS_IS_PUT_RESOURCE_STREAMING";
    private StringEntity content = new StringEntity(contentStr);
    private String exceptionStr = "EXCEPTION";
    private StringEntity exception = new StringEntity(exceptionStr);
    private Path realPath;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        kylinConfig = KylinConfig.getInstanceFromEnv();
        ResourceStoreTest.replaceMetadataUrl(kylinConfig, ResourceStoreTest.mockUrl("hdfs", kylinConfig));
        store = ResourceStore.getStore(kylinConfig);
        realPath = store.resourcePath(path);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
        store.deleteResource(path);
        content.setLastModified(0);
        HadoopUtil.setCurrentConfiguration(new Configuration());
    }

    @Test
    public void testBasic() throws Exception {
        ResourceStoreTest.testAStore(ResourceStoreTest.mockUrl("hdfs", kylinConfig), kylinConfig);
    }

    @Ignore
    @Test
    public void performanceTest() throws Exception {
        //test hdfs performance
        ResourceStoreTest.testPerformance(ResourceStoreTest.mockUrl("hdfs", kylinConfig), kylinConfig);
        //test hbase
        ResourceStoreTest.testPerformance(ResourceStoreTest.mockUrl("hbase", kylinConfig), kylinConfig);
    }

    @Test
    public void testPutResourceStreaming() throws Exception {
        ResourceStoreTest.replaceMetadataUrl(kylinConfig, ResourceStoreTest.mockUrl("hdfs", kylinConfig));
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
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
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
    public void testWithExcetion() throws IOException {
        runExceptionCase("org.apache.kylin.storage.DistributedExceptionFileSystem.RenameExceptionFileSystem",
                "TEST RENAME EXCEPTION");

        runExceptionCase("org.apache.kylin.storage.DistributedExceptionFileSystem.SetTimesExceptionFileSystem",
                "TEST SET TIMES EXCEPTION");
    }

    private void runExceptionCase(String className, String expectedMessage) throws IOException {
        // write a resource
        long ts = store.putResourceStreaming(path, content, StringEntity.serializer);

        // use exception file system
        KylinConfig.destroyInstance();
        KylinConfig exceptionConfig = KylinConfig.getInstanceFromEnv();
        ResourceStoreTest.replaceMetadataUrl(exceptionConfig, ResourceStoreTest.mockUrl("hdfs", exceptionConfig));

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", className);
        conf.set("fs.hdfs.impl.disable.cache", "true");
        HadoopUtil.setCurrentConfiguration(conf);
        ResourceStore exceptionStore = ResourceStore.getStore(exceptionConfig);

        ResourceStore.Checkpoint cp = exceptionStore.checkpoint();
        try {
            exception.setLastModified(ts);
            exceptionStore.putResourceStreaming(path, exception, StringEntity.serializer);
        } catch (IOException e) {
            cp.rollback();
            assertFalse(HadoopUtil.getWorkingFileSystem().exists(exceptionStore.resourcePath(tmpPath)));
            assertEquals(expectedMessage, e.getMessage());
            exception.setLastModified(0);
        }

        // check rollback success
        StringEntity t = store.getResource(path, StringEntity.class, StringEntity.serializer);
        assertEquals(content, t);

        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        assertTrue(fileSystem.exists(realPath));

        FSDataInputStream in = fileSystem.open(realPath);
        assertEquals(contentStr, in.readUTF());
        in.close();
    }
}
