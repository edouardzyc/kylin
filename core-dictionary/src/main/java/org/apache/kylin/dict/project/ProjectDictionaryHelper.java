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

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.TrieDictionary;
import org.apache.kylin.dict.TrieDictionaryForest;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;

import com.google.common.collect.Lists;

final public class ProjectDictionaryHelper {

    public static boolean useProjectDictionary(Dictionary<String> dictionary) {
        return (dictionary instanceof TrieDictionaryForest || dictionary instanceof TrieDictionary);
    }

    public static boolean useProjectDictionary(String dictionaryClazz) {
        if (dictionaryClazz.endsWith("AppendTrieDictionary")) {
            return false;
        }
        return (dictionaryClazz.endsWith("TrieDictionaryForest") || dictionaryClazz.endsWith("TrieDictionary"));
    }

    public static int[] genOffset(DictionaryInfo small, DictionaryInfo big) {
        Dictionary<String> smallDict = small.getDictionaryObject();
        Dictionary<String> bigDict = big.getDictionaryObject();
        int smallMin = smallDict.getMinId();
        int smallMax = smallDict.getMaxId();
        int[] mapping = new int[smallMax - smallMin + 1];
        for (int i = smallMin; i <= smallMax; i++) {
            mapping[i - smallMin] = bigDict.getIdFromValue(smallDict.getValueFromId(i));
        }
        return mapping;
    }

    public static boolean container(Dictionary<String> big, Dictionary<String> small) {
        for (int i = small.getMinId(); i < small.getMaxId(); i++) {
            if (!big.containsValue(small.getValueFromId(i))) {
                return false;
            }
        }
        return true;
    }

    public static List<Pair<String, String>> checkDict(DictionaryInfo originDict,
            DisguiseTrieDictionary<String> dictionary) {
        List<Pair<String, String>> pairs = Lists.newArrayList();
        int minId = originDict.getDictionaryObject().getMinId();
        int maxId = originDict.getDictionaryObject().getMaxId();
        for (int i = minId; i <= maxId; i++) {
            String valueFromId = originDict.getDictionaryObject().getValueFromId(i);
            String valueFromId1 = dictionary.getValueFromId(i);
            if (!valueFromId.equals(valueFromId1)) {
                pairs.add(new Pair<>(valueFromId, valueFromId1));
            }
        }
        return pairs;
    }

    public static void printToSOUT(String resourceIdentify, String version, List<Pair<String, String>> pairs) {
        if (pairs.size() > 0) {
            System.out.println("The dictionary is : " + resourceIdentify);
            System.out.println("Current projection dictionary version is : " + version);
            for (Pair<String, String> pair : pairs) {
                System.out
                        .println("The origin value and project value is  :  " + pair.getKey() + " " + pair.getValue());
            }
        }
    }

    public static void printToLog(String resourceIdentify, String version, List<Pair<String, String>> pairs,
            Logger logger) {
        if (pairs.size() > 0) {
            logger.info("The dictionary is : " + resourceIdentify);
            logger.info("Current projection dictionary version is : " + version);
            for (Pair<String, String> pair : pairs) {
                logger.info("The origin value and project value is  :  " + pair.getKey() + " " + pair.getValue());
            }
        }
    }

    public static class PathBuilder {

        public final static String DICT_DATA = "/data.pdict";
        public final static String SDICT_DATA = "/data.sdict";
        public final static String SPARDER_SDICT_BASE_DIR = ResourceStore.SPARDER_DICT_RESOURCE_ROOT;
        public final static String SDICT_ZIP_DIR = "/sdict.zip";
        public final static String SDICT_DIR = "/sdict";
        public final static String SPARDER_DIR = "/sparder";
        public final static String SPARDER_DICT_ROOT = SPARDER_DIR + SDICT_DIR;
        public final static String PROJECT_DICT_FLAG = ResourceStore.PROJECT_DICT_RESOURCE_ROOT + "/migrate_ok";
        public final static String PROJECT_DICT_DIR = ResourceStore.PROJECT_DICT_RESOURCE_ROOT + "/data";

        public static String sourceIdentifier(String project, DictionaryInfo dictionaryInfo) {
            return project + "/" + dictionaryInfo.getSourceTable() + "/" + dictionaryInfo.getSourceColumn();
        }

        public static String segmentPatchPath(String sourceIdentify, String uuid, long version) {
            return PROJECT_DICT_DIR + "/" + sourceIdentify + "/segment/" + uuid + "_" + version;
        }

        public static String versionKey(String sourceIdentify) {
            return sourceIdentify.replaceAll("/", ".");
        }

        public static String dataPath(String sourceIdentify, long version) {
            return PROJECT_DICT_DIR + "/" + sourceIdentify + "/" + version + DICT_DATA;
        }

        public static String patchPath(String sourceIdentify, long currentVersion, long toVersion) {
            return PROJECT_DICT_DIR + "/" + sourceIdentify + "/" + toVersion + "/" + currentVersion + "-" + toVersion;
        }

        public static String sDictPath(String sourceIdentify, long currentVersion) {
            String hdfsWorkingDirectory = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
            return hdfsWorkingDirectory + SPARDER_SDICT_BASE_DIR + "/" + sourceIdentify + "/" + currentVersion
                    + SDICT_DATA;
        }

        public static String versionPath(String sourceIdentify) {
            return PROJECT_DICT_DIR + "/metadata/" + ProjectDictionaryHelper.PathBuilder.versionKey(sourceIdentify)
                    + MetadataConstants.TYPE_VERSION;
        }
    }
}
