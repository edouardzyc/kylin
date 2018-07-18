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

package org.apache.kylin.dict;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class SDictTest {
    @Test
    public void testGet() throws IOException {
        String[] dict = new String[100];
        for (int i = 0; i < dict.length; i++) {
            dict[i] = gen();
        }

        SDict w = new SDict(dict);
        File f = File.createTempFile("dict", ".dict");
        f.deleteOnExit();
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(f))) {
            w.write(out);
        }

        SDict  d1 = new SDict(f.getAbsolutePath());
        d1.init();

        Assert.assertArrayEquals(dict[99].getBytes(), d1.getValueBytesFromIdImpl(99));
        Assert.assertArrayEquals(dict[50].getBytes(), d1.getValueBytesFromIdImpl(50));
        Assert.assertArrayEquals(dict[0].getBytes(), d1.getValueBytesFromIdImpl(0));

        SaveMemDict d2 = new SaveMemDict(f.getAbsolutePath());
        Assert.assertArrayEquals(dict[99].getBytes(), d2.getValueBytesFromIdImpl(99));
        Assert.assertArrayEquals(dict[50].getBytes(), d2.getValueBytesFromIdImpl(50));
        Assert.assertArrayEquals(dict[0].getBytes(), d2.getValueBytesFromIdImpl(0));
    }

    @Test
    public void testWrap() throws IOException {
        TrieDictionaryBuilder<String> b = new TrieDictionaryBuilder<String>(new StringBytesConverter());
        int capacity = 100;
        Set<String> uniqueValue = new HashSet<>(capacity);
        while (uniqueValue.size() < capacity) {
            uniqueValue.add(gen());
        }

        Assert.assertEquals(100, uniqueValue.size());
        for (String v : uniqueValue) {
            b.addValue(v);
        }

        TrieDictionary<String> d = b.build(0);
        SDict dict = SDict.wrap(d);

        // test write and read
        File f = File.createTempFile("dict", ".dict");
        f.deleteOnExit();
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(f))) {
            dict.write(out);
        }

        SDict r = new SDict(f.getAbsolutePath());
        r.init();

        Assert.assertArrayEquals(d.getValueByteFromId(99), r.getValueBytesFromIdImpl(99));
        Assert.assertArrayEquals(d.getValueByteFromId(50), r.getValueBytesFromIdImpl(50));
        Assert.assertArrayEquals(d.getValueByteFromId(0), r.getValueBytesFromIdImpl(0));
    }

    @Test
    @Ignore
    public void testMultiThread() throws IOException, InterruptedException {
        int cap = 1000 * 1000;
        String[] dict = new String[cap];
        for (int i = 0; i < dict.length; i++) {
            dict[i] = gen();
        }

        SDict w = new SDict(dict);
        File f = File.createTempFile("dict", ".dict");
        f.deleteOnExit();
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(f))) {
            w.write(out);
        }
        System.out.println("write down.");

        SDict r = new SDict(f.getAbsolutePath());
        r.init();

        int nThreads = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
        for (int i = 0; i < nThreads; i++){
            executorService.execute(new TestRunnable(r, dict));
        }

        Thread.sleep(1000 * 200);
        executorService.shutdown();
    }

    class TestRunnable implements Runnable{
        private SDict d;
        private String[] ori;

        TestRunnable(SDict r, String[] ori) {
            this.d = r;
            this.ori = ori;
        }

        public void run(){
            int index = new Random().nextInt(1000 * 1000);
            Assert.assertArrayEquals(ori[index].getBytes(), d.getValueBytesFromIdImpl(index));
        }
    }

    @Test
    @Ignore
    public void benchmark() throws IOException {
        int cap = 1000 * 1000;
        String[] dict = new String[cap];
        for (int i = 0; i < dict.length; i++) {
            dict[i] = gen();
        }

        SDict w = new SDict(dict);
        File f = File.createTempFile("dict", ".dict");
        f.deleteOnExit();
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(f))) {
            w.write(out);
        }

        SDict r = new SDict(f.getAbsolutePath());
        r.init();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < cap; i++) {
            r.getValueBytesFromIdImpl(new Random().nextInt(cap)); //1 million read, 399ms
        }
        System.out.println("duration:" + (System.currentTimeMillis() - t1));
    }

    private static String gen() {
        return RandomStringUtils.randomAlphanumeric(new Random().nextInt(100) + 1);
    }
}
