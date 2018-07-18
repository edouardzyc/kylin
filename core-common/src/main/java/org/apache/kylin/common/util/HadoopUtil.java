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

package org.apache.kylin.common.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class HadoopUtil {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HadoopUtil.class);
    private static final transient ThreadLocal<Configuration> hadoopConfig = new ThreadLocal<>();
    private static final Configuration confBase = new Configuration();

    static{
        confBase.get(""); //trigger init of Configuration
    }

    public static void setCurrentConfiguration(Configuration conf) {
        hadoopConfig.set(conf);
    }

    public static Configuration getCurrentConfiguration() {
        if (hadoopConfig.get() == null) {
            Configuration conf = healSickConfig(new Configuration(confBase));// use copy constructor to speed up (parsing xml is slow)
            // do not cache this conf, or will affect following mr jobs
            return conf;
        }
        Configuration conf = hadoopConfig.get();
        return conf;
    }

    public static Configuration healSickConfig(Configuration conf) {
        // https://issues.apache.org/jira/browse/KYLIN-953
        if (StringUtils.isBlank(conf.get("hadoop.tmp.dir"))) {
            conf.set("hadoop.tmp.dir", "/tmp");
        }
        if (StringUtils.isBlank(conf.get("hbase.fs.tmp.dir"))) {
            conf.set("hbase.fs.tmp.dir", "/tmp");
        }
        //  https://issues.apache.org/jira/browse/KYLIN-3064
        conf.set("yarn.timeline-service.enabled", "false");

        return conf;
    }

    public static FileSystem getWorkingFileSystem() throws IOException {
        return getFileSystem(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory(null));
    }

    public static FileSystem getWorkingFileSystem(Configuration conf) throws IOException {
        Path workingPath = new Path(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory(null));
        return getFileSystem(workingPath, conf);
    }

    public static FileSystem getReadFileSystem() throws IOException {
        Configuration conf = getCurrentConfiguration();
        return getReadFileSystem(conf);
    }

    public static FileSystem getReadFileSystem(Configuration conf) throws IOException {
        Path parquetReadPath = new Path(KylinConfig.getInstanceFromEnv().getReadHdfsWorkingDirectory(null));
        return getFileSystem(parquetReadPath, conf);
    }

    public static FileSystem getFileSystem(String path) throws IOException {
        return getFileSystem(new Path(makeURI(path)));
    }

    public static FileSystem getFileSystem(String path, Configuration conf) throws IOException {
        return getFileSystem(new Path(makeURI(path)), conf);
    }

    public static FileSystem getFileSystem(Path path) throws IOException {
        Configuration conf = getCurrentConfiguration();
        return getFileSystem(path, conf);
    }

    public static FileSystem getFileSystem(Path path, Configuration conf) {
        try {
            return path.getFileSystem(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static URI makeURI(String filePath) {
        try {
            return new URI(fixWindowsPath(filePath));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Cannot create FileSystem from URI: " + filePath, e);
        }
    }

    public static String fixWindowsPath(String path) {
        // fix windows path
        if (path.startsWith("file://") && !path.startsWith("file:///") && path.contains(":\\")) {
            path = path.replace("file://", "file:///");
        }
        if (path.startsWith("file:///")) {
            path = path.replace('\\', '/');
        }
        return path;
    }

    /**
     * @param table the identifier of hive table, in format <db_name>.<table_name>
     * @return a string array with 2 elements: {"db_name", "table_name"}
     */
    public static String[] parseHiveTableName(String table) {
        int cut = table.indexOf('.');
        String database = cut >= 0 ? table.substring(0, cut).trim() : "DEFAULT";
        String tableName = cut >= 0 ? table.substring(cut + 1).trim() : table.trim();

        return new String[] { database, tableName };
    }

    public static void deletePath(Configuration conf, Path path) throws IOException {
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    public static byte[] toBytes(Writable writable) {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bout);
            writable.write(out);
            out.close();
            bout.close();
            return bout.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path getFilterOnlyPath(FileSystem fs, Path baseDir, final String filter) throws IOException {
        if (fs.exists(baseDir) == false) {
            return null;
        }

        FileStatus[] fileStatus = fs.listStatus(baseDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(filter);
            }
        });

        if (fileStatus.length == 1) {
            return fileStatus[0].getPath();
        } else {
            return null;
        }
    }

    public static Path[] getFilteredPath(FileSystem fs, Path baseDir, final String prefix) throws IOException {
        if (fs.exists(baseDir) == false) {
            return null;
        }

        FileStatus[] fileStatus = fs.listStatus(baseDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(prefix);
            }
        });

        Path[] result = new Path[fileStatus.length];
        for (int i = 0; i < fileStatus.length; i++) {
            result[i] = fileStatus[i].getPath();
        }
        return result;
    }

    public static Configuration loadConfig(String path) {
        Configuration config = new Configuration(false);
        try {
            config.addResource(new FileInputStream(new File(path)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Job config file is not exists, path: " + path);
        }
        return config;
    }

    public static String getPathWithoutScheme(String path) {
        if (path.startsWith("file://") || path.startsWith("maprfs://"))
            return path;

        if (path.startsWith("file:")) {
            path = path.replace("file:", "file://");
        } else if (path.startsWith("maprfs:")) {
            path = path.replace("maprfs:", "maprfs://");
        } else {
            path = Path.getPathWithoutSchemeAndAuthority(new Path(path)).toString() + "/";
        }
        return path;
    }

    public static FileSystem getJdbcFileSystem() throws IOException {
        Configuration conf = getCurrentConfiguration();
        return getJdbcFileSystem(conf);
    }

    public static FileSystem getJdbcFileSystem(Configuration conf) throws IOException {
        Path jdbcPath = new Path(KylinConfig.getInstanceFromEnv().getJdbcHdfsWorkingDirectory());
        return getFileSystem(jdbcPath, conf);
    }

    public static String getPathWithReadScheme(String path) {
        String scheme = KylinConfig.getInstanceFromEnv().getPrefixReadScheme();
        if (isSpecialFs(scheme)) {
            return path;
        }
        return scheme + Path.getPathWithoutSchemeAndAuthority(new Path(path));
    }

    public static String getPathWithReadScheme(String[] paths) {
        List pathList = Lists.newArrayList();
        for (String path : paths) {
            pathList.add(getPathWithReadScheme(path));
        }
        return StringUtils.join(pathList, ",");
    }

    public static String getPathWithWorkingScheme(String path) {
        String scheme = KylinConfig.getInstanceFromEnv().getPrefixWorkingScheme();
        if (isSpecialFs(scheme)) {
            return path;
        }
        return scheme + Path.getPathWithoutSchemeAndAuthority(new Path(path));
    }

    private static boolean isSpecialFs(String scheme) {
        if (scheme.startsWith("file://") || scheme.startsWith("maprfs://"))
            return true;

        return false;
    }
}
