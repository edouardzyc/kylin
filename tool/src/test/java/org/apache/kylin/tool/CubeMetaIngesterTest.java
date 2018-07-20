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

package org.apache.kylin.tool;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class CubeMetaIngesterTest extends LocalFileMetadataTestCase {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testHappyIngest() {
        String srcPath = Thread.currentThread().getContextClassLoader().getResource("cloned_cube_and_model.zip")
                .getPath();
        CubeMetaIngester.main(new String[] { "-project", "default", "-srcPath", srcPath, "-overwriteTables", "true",
                "-forceIngest", "true", "-restoreType", "cube" });
        ProjectInstance project = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject("default");
        Assert.assertEquals(1, Collections.frequency(project.getTables(), "DEFAULT.TEST_KYLIN_FACT"));
        Assert.assertTrue(project.getModels().contains("cloned_model"));
        Assert.assertTrue(
                project.getRealizationEntries().contains(RealizationEntry.create(RealizationType.CUBE, "cloned_cube")));

        getTestConfig().clearManagers();
        CubeInstance instance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube("cloned_cube");
        Assert.assertTrue(instance != null);
    }

    @Test
    public void testHappyIngest2() {
        String srcPath = Thread.currentThread().getContextClassLoader().getResource("benchmark_meta.zip").getPath();
        CubeMetaIngester.main(new String[] { "-project", "default", "-srcPath", srcPath, "-overwriteTables", "true",
                "-forceIngest", "true", "-restoreType", "cube" });
        ProjectInstance project = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject("default");
        Assert.assertEquals(1, Collections.frequency(project.getTables(), "SSB.CUSTOMER"));
        Assert.assertTrue(project.getModels().contains("benchmark_model"));
        Assert.assertTrue(project.getRealizationEntries()
                .contains(RealizationEntry.create(RealizationType.CUBE, "benchmark_cube")));

        getTestConfig().clearManagers();
        CubeInstance instance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube("benchmark_cube");
        Assert.assertTrue(instance != null);
    }

    @Test
    public void testBadIngest() throws IOException {
        thrown.expect(RuntimeException.class);

        //should not break at table duplicate check, should fail at model duplicate check
        thrown.expectCause(new BaseMatcher<Throwable>() {
            @Override
            public boolean matches(Object item) {
                if (item instanceof IllegalStateException) {
                    if (((IllegalStateException) item).getMessage().startsWith(
                            "The model calcs_tdvt cannot exist in multiple projects, please resolve the conflicts. ")) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
            }
        });

        String srcPath = doExtractorProject();
        CubeMetaIngester.main(new String[] { "-project", "default", "-srcPath", srcPath, "-overwriteTables", "true",
                "-forceIngest", "false", "-restoreType", "project" });

    }

    @Test
    public void testProjectNotExist() {

        thrown.expect(RuntimeException.class);
        thrown.expectCause(CoreMatchers.<IllegalStateException> instanceOf(IllegalStateException.class));

        String srcPath = this.getClass().getResource("/cloned_cube_meta.zip").getPath();
        CubeMetaIngester.main(new String[] { "-project", "Xdefault", "-srcPath", srcPath, "-overwriteTables", "true",
                "-forceIngest", "true", "-restoreType", "cube" });
    }

    @Test
    public void testBackupProjectNotExist() throws IOException {
        String srcPath = doExtractorProject();
        // delete the project
        ProjectManager projectManager = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance project = projectManager.getProject("default");

        DataModelManager modelManager = DataModelManager.getInstance(KylinConfig.getInstanceFromEnv());
        for (DataModelDesc modelDesc : modelManager.getModels("default")) {
            modelManager.dropModel(modelDesc);
        }

        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        for (RealizationEntry realization : project.getRealizationEntries()) {
            if (realization.getType() == RealizationType.CUBE) {
                cubeManager.dropCube(realization.getRealization(), true);
            }

        }

        ProjectInstance projectInstance = projectManager.getProject("default");
        projectInstance.getRealizationEntries().clear();

        projectManager.dropProject("default");

        project = projectManager.getProject("default");
        Assert.assertNull(project);
        // backup project
        CubeMetaIngester.main(new String[] { "-project", "default", "-srcPath", srcPath, "-overwriteTables", "true",
                "-forceIngest", "true", "-restoreType", "project" });

        projectManager.reloadProjectQuietly("default");
        project = projectManager.getProject("default");

        Assert.assertNotNull(project);

    }

    @Test
    public void testBadProjectIngest() throws IOException {
        thrown.expect(RuntimeException.class);

        //should not break at table duplicate check, should fail at model duplicate check
        thrown.expectCause(new BaseMatcher<Throwable>() {
            @Override
            public boolean matches(Object item) {
                if (item instanceof IllegalStateException) {
                    if (((IllegalStateException) item).getMessage().startsWith(
                            "The model calcs_tdvt cannot exist in multiple projects, please resolve the conflicts. ")) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
            }
        });

        String srcPath = doExtractorProject();

        ProjectManager projectManager = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance project = projectManager.getProject("default");
        project.removeModel("calcs_tdvt");
        projectManager.createProject("default1", "ADMIN", "", null);
        projectManager.addModelToProject("calcs_tdvt", "default1");

        CubeMetaIngester.main(new String[] { "-project", "default", "-srcPath", srcPath, "-overwriteTables", "true",
                "-forceIngest", "true", "-restoreType", "project" });

    }

    @Test
    public void testBadCubeIngest() throws IOException {
        thrown.expect(RuntimeException.class);

        //should not break at table duplicate check, should fail at model duplicate check
        thrown.expectCause(new BaseMatcher<Throwable>() {
            @Override
            public boolean matches(Object item) {
                if (item instanceof IllegalStateException) {
                    if (((IllegalStateException) item).getMessage()
                            .startsWith("The cube ssb cannot exist in multiple models, please resolve the conflicts. ")) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
            }
        });

        String srcPath = doExtractorProject();
        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubeInstance cube = cubeManager.getCube("ssb");
        cube.getDescriptor().setModelName("ci_inner_join_model");
        CubeMetaIngester.main(new String[] { "-project", "default", "-srcPath", srcPath, "-overwriteTables", "true",
                "-forceIngest", "true", "-restoreType", "project" });
    }

    @Test
    public void testBadModelIngest() throws IOException {
        thrown.expect(RuntimeException.class);

        //should not break at table duplicate check, should fail at model duplicate check
        thrown.expectCause(new BaseMatcher<Throwable>() {
            @Override
            public boolean matches(Object item) {
                if (item instanceof IllegalStateException) {
                    if (((IllegalStateException) item).getMessage()
                            .startsWith("The model calcs_tdvt cannot exist in multiple projects, please resolve the conflicts. ")) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public void describeTo(Description description) {
            }
        });

        String srcPath = doExtractorProject();
        ProjectManager projectManager = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.createProject("default1", "ADMIN", "", null);
        CubeMetaIngester.main(new String[] { "-project", "default1", "-srcPath", srcPath, "-overwriteTables", "true",
                "-forceIngest", "true", "-restoreType", "project" });
    }

    private String doExtractorProject() throws IOException {
        // Does not support ingest hybrid yet so removeProject before extractorProject
        ProjectManager projectManager = ProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        projectManager.removeRealizationsFromProjects(RealizationType.HYBRID, "ci_inner_join_hybrid");

        folder.create();
        File tempDir = folder.getRoot();
        String tempDirAbsPath = tempDir.getAbsolutePath();
        List<String> args = new ArrayList<>();
        args.add("-destDir");
        args.add(tempDirAbsPath);
        args.add("-project");
        args.add("default");
        args.add("-compress");
        args.add("false");
        args.add("-includeSegments");
        args.add("false");
        String[] cubeMetaArgs = new String[args.size()];
        args.toArray(cubeMetaArgs);

        CubeMetaExtractor cubeMetaExtractor = new CubeMetaExtractor();
        cubeMetaExtractor.execute(cubeMetaArgs);

        File[] files = tempDir.listFiles();
        Preconditions.checkState(files.length == 1);
        String testDir = "test.zip";
        CliCommandExecutor cli = new CliCommandExecutor();
        String cmd = "cd " + files[0].getParent() + " && zip -r " + testDir + " " + files[0].getName();
        cli.execute(cmd);

        return files[0].getParent() + "/" + testDir;
    }

}