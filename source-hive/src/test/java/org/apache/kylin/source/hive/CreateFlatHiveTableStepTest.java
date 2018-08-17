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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CreateFlatHiveTableStepTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCreateHiveTableDirIfNeed()
            throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        String absolutePath = new File("./hivedir/").getAbsolutePath();
        if (absolutePath.startsWith("/"))
            absolutePath = "file://" + absolutePath;
        else
            absolutePath = "file:///" + absolutePath;
        Path jobWorkDirPath = new Path(absolutePath);
        KylinConfig kylinConfig = mock(KylinConfig.class);
        CreateFlatHiveTableStep createFlatHiveTableStep = new CreateFlatHiveTableStep();
        createFlatHiveTableStep.setWorkingDir(absolutePath);

        try {
            when(kylinConfig.getHiveTableDirCreateFirst()).thenReturn(true);
            Class<CreateFlatHiveTableStep> createFlatHiveTableStepClass = CreateFlatHiveTableStep.class;
            Method declaredMethod = createFlatHiveTableStepClass.getDeclaredMethod("createHiveTableDirIfNeed",
                    KylinConfig.class);
            declaredMethod.setAccessible(true);
            declaredMethod.invoke(createFlatHiveTableStep, kylinConfig);
            declaredMethod.setAccessible(false);
            Assert.assertTrue(fileSystem.exists(jobWorkDirPath));
        } finally {
            if (jobWorkDirPath != null)
                fileSystem.deleteOnExit(jobWorkDirPath);
        }
    }

    @Test
    public void testSetWorkingDir() {
        CreateFlatHiveTableStep createFlatHiveTableStep = new CreateFlatHiveTableStep();
        String jobWorkingDir = "/tmp/kylin";
        createFlatHiveTableStep.setWorkingDir(jobWorkingDir);
        Assert.assertEquals(createFlatHiveTableStep.getWorkingDir(), jobWorkingDir);
    }

    @Test
    public void testSetCreateTableStatement() {
        CreateFlatHiveTableStep createFlatHiveTableStep = new CreateFlatHiveTableStep();
        String createTableStatement = "HiveRedistributeData";
        createFlatHiveTableStep.setCreateTableStatement(createTableStatement);
        Assert.assertEquals(createFlatHiveTableStep.getCreateTableStatement(), createTableStatement);
    }

    @Test
    public void testSetInitStatement() {
        CreateFlatHiveTableStep createFlatHiveTableStep = new CreateFlatHiveTableStep();
        String initStatement = "HiveInit";
        createFlatHiveTableStep.setInitStatement(initStatement);
        Assert.assertEquals(createFlatHiveTableStep.getInitStatement(), initStatement);
    }

}
