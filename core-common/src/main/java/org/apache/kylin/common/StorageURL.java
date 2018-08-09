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

package org.apache.kylin.common;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

/**
 * The object form of metadata/storage URL: IDENTIFIER@SCHEME[,PARAM=VALUE,PARAM=VALUE...]
 * <p>
 * It is not standard URL, but a string of specific format that shares some similar parts with URL.
 * <p>
 * Immutable by design.
 */
public class StorageURL {

    // jdbc:mysql:[loadbalance:]//host1:port[[,host2:port]...]/database[?propertyName1=propertyValue1[&propertyName2=propertyValue2]...]
    private static final Pattern MYSQL_PATTERN = Pattern.compile("(url)=(jdbc:mysql:.*?//.*?/.*?),");

    private static final Pattern SQLSERVER_PATTERN = Pattern.compile("url=jdbc:sqlserver://.*?");

    // for test
    private static final Pattern H2_PATTERN = Pattern.compile("url=jdbc:h2.*?");

    private static final LoadingCache<String, StorageURL> cache = CacheBuilder.newBuilder()//
            .maximumSize(100)//
            .build(new CacheLoader<String, StorageURL>() {
                @Override
                public StorageURL load(String metadataUrl) throws Exception {
                    return new StorageURL(metadataUrl);
                }
            });

    public static StorageURL valueOf(String metadataUrl) {
        try {
            return cache.get(metadataUrl);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    // ============================================================================

    final String identifier;
    final String scheme;
    final Map<String, String> params;

    // package private for test
    StorageURL(String metadataUrl) {
        int tmpCut = -1;

        final int headCut = metadataUrl.indexOf(",");
        final String headSplit = headCut > 0 ? metadataUrl.substring(0, headCut).trim() : metadataUrl;

        // identifier @ scheme
        tmpCut = headSplit.lastIndexOf('@');
        if (tmpCut < 0) {
            this.identifier = headSplit.trim().isEmpty() ?
                    "kylin_metadata" : headSplit.trim();

            this.scheme = "";

        } else {
            this.identifier = headSplit.substring(0, tmpCut).trim().isEmpty() ?
                    "kylin_metadata" : headSplit.substring(0, tmpCut).trim();

            this.scheme = headSplit.substring(tmpCut + 1).trim();
        }

        // param = value
        final Map<String, String> entries = new LinkedHashMap<>();
        String paramSplit = headCut > 0 ? metadataUrl.substring(headCut + 1).trim() : "";
        if (paramSplit.isEmpty()) {
            this.params = ImmutableMap.copyOf(entries);
            return;
        }

        // Special treatment mysql cluster url when the schema is jdbc
        if ("jdbc".equals(this.scheme)) {
            if (!SQLSERVER_PATTERN.matcher(paramSplit).matches() && !H2_PATTERN.matcher(paramSplit).matches()) {
                final Matcher matcher = MYSQL_PATTERN.matcher(paramSplit);
                if (!matcher.find()) {
                    throw new IllegalArgumentException("Missing or illegal \"url\" parameter, see the documentation for details");
                }

                entries.put(matcher.group(1), matcher.group(2));
                paramSplit = paramSplit.replace(matcher.group(), "");
            }
        }

        // normal param
        for (String param : paramSplit.split(",")) {
            tmpCut = param.indexOf('=');
            if (tmpCut < 0) {
                entries.put(param.trim(), "");
            } else {
                entries.put(param.substring(0, tmpCut).trim(), param.substring(tmpCut + 1).trim());
            }
        }
        this.params = ImmutableMap.copyOf(entries);
    }

    public StorageURL(String identifier, String scheme, Map<String, String> params) {
        this.identifier = identifier;
        this.scheme = scheme;
        this.params = ImmutableMap.copyOf(params);
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getScheme() {
        return scheme;
    }

    public boolean containsParameter(String k) {
        return params.containsKey(k);
    }

    public String getParameter(String k) {
        return params.get(k);
    }

    public Map<String, String> getAllParameters() {
        return params;
    }

    public StorageURL copy(Map<String, String> params) {
        return new StorageURL(identifier, scheme, params);
    }

    @Override
    public String toString() {
        String str = identifier;
        if (!scheme.isEmpty())
            str += "@" + scheme;

        for (Entry<String, String> kv : params.entrySet()) {
            str += "," + kv.getKey();
            if (!kv.getValue().isEmpty())
                str += "=" + kv.getValue();
        }
        return str;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((identifier == null) ? 0 : identifier.hashCode());
        result = prime * result + ((params == null) ? 0 : params.hashCode());
        result = prime * result + ((scheme == null) ? 0 : scheme.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StorageURL other = (StorageURL) obj;
        if (identifier == null) {
            if (other.identifier != null)
                return false;
        } else if (!identifier.equals(other.identifier))
            return false;
        if (params == null) {
            if (other.params != null)
                return false;
        } else if (!params.equals(other.params))
            return false;
        if (scheme == null) {
            if (other.scheme != null)
                return false;
        } else if (!scheme.equals(other.scheme))
            return false;
        return true;
    }

}
