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

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.kylin.common.util.Dictionary;
import org.junit.Assert;
import org.junit.Test;

public class ProjectTrieDictionaryBackwardCompatibilityTest {

    @Test
    public void testSomething() throws IOException {
        FileInputStream open = new FileInputStream("src/test/resources/dict/84b48341-af0d-4290-bb8a-04b23935a5bc.dict");
        ProjectDictionaryInfo dictionaryInfo = ProjectDictionaryInfoSerializer.FULL_SERIALIZER
                .deserialize(new DataInputStream(open));
        Dictionary<String> dictionary = dictionaryInfo.getDictionaryObject();
        
        Assert.assertEquals(1582, dictionary.getIdFromValue("28278.0"));
        
        for (int i = dictionary.getMinId(); i <= dictionary.getMaxId(); i++) {
            String v = dictionary.getValueFromId(i);
            Assert.assertEquals(i, dictionary.getIdFromValue(v));
        }

    }
    
}