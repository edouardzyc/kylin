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

package org.apache.kylin.common.persistence;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class PushDownDFSResourceStore extends ResourceStore {
    private static final Logger logger = LoggerFactory.getLogger(HDFSResourceStore.class);

    protected PushDownDFSResourceStore(KylinConfig kylinConfig) {
        super(kylinConfig);
    }

    protected DataOutputStream getStreamImpl(String path) throws IOException {
        Path realPath = resourcePath(path);
        FileSystem fileSystem = getFileSystem(realPath);
        if (fileSystem.exists(realPath)) {
            logger.info("File already exists {}, delete it.", realPath);
            fileSystem.delete(realPath, true);
        }
        return fileSystem.create(realPath, true);
    }

    protected void commitChange(String tmpPath, String resPath, long ts) throws IOException {
        Path tmpRealPath = resourcePath(tmpPath);
        Path resRealPath = resourcePath(resPath);
        FileSystem fileSystem = getFileSystem(tmpRealPath);
        if (fileSystem.exists(resRealPath)) {
            logger.info("Real resource file exists, delete it. Resource file: {} .", resRealPath);
            fileSystem.delete(resRealPath, true);
        }
        if (fileSystem.exists(tmpRealPath)) {
            if (fileSystem.rename(tmpRealPath, resRealPath)) {
                logger.info("Commit change success. Rename temp file: {} to resource file: {} .", tmpRealPath,
                        resRealPath);
            } else {
                throw new IOException(String.format("Rename temp file failed. Temp file: %s . Resource file: %s .",
                        tmpRealPath, resRealPath));
            }
        } else {
            throw new IOException(String.format("Temp file not exists. File name: %s .", tmpRealPath));
        }
    }

    protected FileSystem getFileSystem(Path path) {
        return HadoopUtil.getFileSystem(path);
    }

    protected void deleteTempResourceFile(Path tmpRealPath) {
        logger.info("Delete tmp file {} .", tmpRealPath);
        FileSystem fileSystem = getFileSystem(tmpRealPath);
        try {
            if (fileSystem.exists(tmpRealPath)) {
                fileSystem.delete(tmpRealPath, true);
                logger.info("Delete temp file success. Temp file: {} .", tmpRealPath);
            } else {
                logger.warn("{} is not exists in the file system.", tmpRealPath);
            }
        } catch (Throwable e) {
            logger.error("Delete temp file failed, ignore this exception.", e);
        }
    }
}
