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

package org.apache.kylin.dict.project;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryInfoSerializer;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.TrieDictionaryForest;
import org.apache.kylin.dict.TrieDictionaryForestBuilder;
import org.apache.kylin.dict.TrieDictionaryForestTest;
import org.apache.kylin.dict.utils.RandomStrings;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.source.IReadableTable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import me.lemire.integercompression.differential.IntegratedBinaryPacking;
import me.lemire.integercompression.differential.IntegratedVariableByte;
import me.lemire.integercompression.differential.SkippableIntegratedComposition;

@Ignore
public class ProjectTrieDictionaryTest extends LocalFileMetadataTestCase {

    static String base = "abcdefghijklmnopqrstuvwxyz0123456789";
    static Random random = new Random();
    DictionaryManager dictionaryManager;

    String baseDir = System.getProperty("user.dir");


    public static String getRandomString(int length) { //length表示生成字符串的长度
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            sb.append(base.charAt(number));
        }
        return sb.toString();
    }

    @Before
    public void before() throws Exception {
        staticCreateTestMetadata();
        dictionaryManager = DictionaryManager.getInstance(KylinConfig.getInstanceFromEnv());

    }

    @Test
    public void testDictMerge() throws IOException {
        DictionaryInfo dictionaryInfo = createDictionary(10);
        DictionaryInfo dictionaryInfo2 = createDictionary(100);
        Dictionary mergedDict = DictionaryGenerator.mergeDictionaries(DataType.getType(dictionaryInfo.getDataType()),
                Lists.newArrayList(dictionaryInfo, dictionaryInfo2));
        Assert.assertTrue(mergedDict.contains(dictionaryInfo.getDictionaryObject())
                && mergedDict.contains(dictionaryInfo2.getDictionaryObject()));
    }

    @Test
    public void testMerge() throws IOException {
        DictionaryInfo dict = createDictionary(100);
        DictionaryInfo dict2 = createDictionary(1000);
        Dictionary<String> mergeDict = DictionaryGenerator.mergeDictionaries(DataType.getType("string"),
                Lists.newArrayList(dict, dict2));
        Dictionary<String> dictionaryObject = dict.getDictionaryObject();
        int[] ints = new int[dictionaryObject.getSize()];
        int minId = dictionaryObject.getMinId();
        for (int i = minId; i <= dictionaryObject.getMaxId(); i++) {
            ints[i - minId] = mergeDict.getIdFromValue(dictionaryObject.getValueFromId(i));
        }
        int anInt = ints[10];
        Assert.assertEquals(Objects.requireNonNull(dictionaryObject.getValueFromId(minId + 10)),
                Objects.requireNonNull(mergeDict.getValueFromId(anInt)).toString());
        DictionaryInfo newDictInfo = new DictionaryInfo();
        newDictInfo.setDictionaryObject(mergeDict);
        DictionaryInfo newdict2 = createDictionary(10000);
        Dictionary<String> mergeDict2 = DictionaryGenerator.mergeDictionaries(DataType.getType("string"),
                Lists.newArrayList(newdict2, newDictInfo));
        int[] newInts = new int[ints.length];
        for (int i = 0; i < newInts.length; i++) {
            newInts[i] = mergeDict2.getIdFromValue(mergeDict.getValueFromId(ints[i]));
        }
        Assert.assertEquals(Objects.requireNonNull(dictionaryObject.getValueFromId(minId + 10)),
                Objects.requireNonNull(mergeDict2.getValueFromId(newInts[10])).toString());
    }

    @Test
    public void testOffset() throws IOException {
        DictionaryInfo small = createDictionary(1000000);
        DictionaryInfo big = createDictionary(2000000);
        DictionaryInfo mergeDictionary = dictionaryManager.mergeDictionary(Lists.newArrayList(small, big));
        int[] mapping = ProjectDictionaryHelper.genOffset(small, mergeDictionary);
        int minId = small.getDictionaryObject().getMinId();
        int maxId = small.getDictionaryObject().getMaxId();
        for (int i = minId; i <= maxId; i++) {
            Assert.assertEquals(small.getDictionaryObject().getValueFromId(i),
                    mergeDictionary.getDictionaryObject().getValueFromId(mapping[i - minId]));
        }

    }

    SkippableIntegratedComposition codec = new SkippableIntegratedComposition(new IntegratedBinaryPacking(),
            new IntegratedVariableByte());

    @Test
    public void testOffsetCompress() throws IOException {
        DictionaryInfo small = createDictionary(1000000);
        DictionaryInfo big = createDictionary(1500000);
        DictionaryInfo mergeDictionary = dictionaryManager.mergeDictionary(Lists.newArrayList(small, big));
        int[] mapping = ProjectDictionaryHelper.genOffset(small, mergeDictionary);
        int[] recovered = IntegratedUtils.unCompress(IntegratedUtils.compress(mapping));
        if (Arrays.equals(mapping, recovered)) {
            System.out.println("data is recovered without loss");
        } else {
            throw new RuntimeException("bug"); // could use assert
        }

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
        StringBuilder stringBuilder = new StringBuilder(outputStream.toByteArray().length);
        for (int map : mapping) {
            dataOutputStream.writeInt(map);
            stringBuilder.append(map);
        }
    }

    @Test
    public void testMVDictionaryInfoSerializer() throws IOException {
        DictionaryInfo dictionaryInfo = createDictionary(100);
        ProjectDictionaryInfo warp = ProjectDictionaryInfo.wrap(dictionaryInfo, 10);
        Path path = new Path(baseDir + "/pd/pd.dict");
        FileSystem fileSystem = path.getFileSystem(new Configuration());
        try {
            FSDataOutputStream open = fileSystem.create(path);
            ProjectDictionaryInfoSerializer.FULL_SERIALIZER.serialize(warp, new DataOutputStream(open));
            open.close();
            ProjectDictionaryInfo projectDictionaryInfo = readDict(path);
            Assert.assertEquals(projectDictionaryInfo.getDictionaryObject().getValueFromId(10),
                    warp.getDictionaryObject().getValueFromId(10));

        } finally {
            fileSystem.deleteOnExit(path);
        }
    }

    @Test
    @Ignore
    public void testSense() throws IOException {
        simulation(1, 1_000_000); // baseline duration:1170
        simulation(1, 10_000); // baseline duration:1146
        simulation(3, 10_000); // duration:3236
        simulation(3, 10_0000); // duration:3516
        simulation(6, 10_000); // duration:6815
        simulation(10, 10_000); // duration:11310
        simulation(20, 10_000); // duration:26704
        simulation(30, 10_000); // duration:41209
        simulation(40, 10_000); // duration:74407
        simulation(50, 10_000); // duration:125427
    }

    private void simulation(int cols, int readOnetime) throws IOException {
        int cap = 1000 * 1000;
        List<Path> paths = new ArrayList<>();
        try {
            for (int i = 0; i < cols; i++) {
                ProjectDictionaryInfo wrap = ProjectDictionaryInfo.wrap(createDictionary(cap, 100), 10);
                Path p = new Path(baseDir + "/dict/test" + i + ".dict");
                paths.add(p);

                FileSystem fileSystem = p.getFileSystem(new Configuration());
                FSDataOutputStream open = fileSystem.create(p);
                ProjectDictionaryInfoSerializer.FULL_SERIALIZER.serialize(wrap, new DataOutputStream(open));
                open.close();
            }

            List<TrieDictionaryForest> dicts = new ArrayList<>();
            for (int i = 0; i < cols; i++) {
                TrieDictionaryForest d = (TrieDictionaryForest) readDict(paths.get(i)).getDictionaryObject();
                d.disableCache();
                dicts.add(d);
            }

            long t1 = System.currentTimeMillis();
            for (int i = 0; i < cap / readOnetime; i++) { // rounds
                for (int k = 0; k < cols; k++) { // simulation n cols
                    for (int j = 0; j < readOnetime; j++) { // n rows per col and round
                        TrieDictionaryForest dict = dicts.get(k);
                        dict.getValueFromId(new Random().nextInt(dict.getSize()));
                    }
                }
            }
            System.out.println("duration:" + (System.currentTimeMillis() - t1));
        } finally {
            FileUtils.deleteDirectory(new File(baseDir + "/mvd"));
        }
    }

    public ProjectDictionaryInfo readDict(Path path) throws IOException {
        FSDataInputStream open = path.getFileSystem(new Configuration()).open(path);
        return ProjectDictionaryInfoSerializer.FULL_SERIALIZER.deserialize(open);
    }

    public Path createAndSaveDictionary(int size, String path) throws IOException {
        DictionaryInfo dictionary = createDictionary(size);
        Path savePath = Path.mergePaths(new Path(baseDir), new Path(path));
        saveDictionary(dictionary, savePath);
        return savePath;
    }

    public DictionaryInfo createDictionary(int size, int maxStrLen) {
        List<String> randomStrings = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            randomStrings.add(RandomStringUtils.randomAlphanumeric(new Random().nextInt(maxStrLen) + 1));
        }

        return build(size, randomStrings);
    }

    public DictionaryInfo createDictionary(int size) {
        RandomStrings randomStrings = new RandomStrings(size);
        return build(size, Lists.newArrayList(randomStrings.iterator()));
    }

    private DictionaryInfo build(int size, List<String> randomStrings) {
        TrieDictionaryForestBuilder<String> dictionaryForestBuilder = TrieDictionaryForestTest
                .newDictBuilder(Lists.newArrayList(randomStrings.iterator()), 0);
        randomStrings.add(null);
        TrieDictionaryForest<String> dict = dictionaryForestBuilder.build();
        DictionaryInfo dictInfo = new DictionaryInfo();
        dictInfo.setDictionaryObject(dict);
        dictInfo.setDictionaryClass(dict.getClass().getCanonicalName());
        IReadableTable.TableSignature tableSignature = new IReadableTable.TableSignature("", size,
                System.currentTimeMillis());
        dictInfo.setSourceTable(UUID.randomUUID().toString());
        dictInfo.setSourceColumn(UUID.randomUUID().toString());
        dictInfo.setInput(tableSignature);
        dictInfo.setDataType("String");
        return dictInfo;
    }

    public void saveDictionary(DictionaryInfo dictionaryInfo, Path path) throws IOException {
        FileSystem fileSystem = HadoopUtil.getFileSystem(path);
        try (FSDataOutputStream open = fileSystem.create(path)) {
            DictionaryInfoSerializer.FULL_SERIALIZER.serialize(dictionaryInfo, new DataOutputStream(open));
        }
    }
}