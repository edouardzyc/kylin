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

package org.apache.kylin.dict.lookup;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.kylin.common.util.ThrowableUtils;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class SnapshotTableV2 extends SnapshotTable {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotTableV2.class);

    private List<String[]> contentsCache;
    private IReadableTable sourceTable;
    private TableDesc sourceTableDesc;
    private String hashAlgorithm = "MD5";
    private GzipCodec codec = new GzipCodec();
    private String standbyDelimiter = "\u0002\u0003";
    private boolean existsStandbyDelimiter = false;
    private boolean existsDelimiter = false;

    public static String VERSION_ID = "V2";

    @JsonProperty("md5")
    protected String md5;

    @JsonProperty("rowCount")
    private int rowCount;

    @JsonProperty("compressionCodecName")
    private String compressionCodecName = "UNCOMPRESSED";

    @JsonProperty("existsNULL")
    private boolean existsNULL = false;

    @JsonProperty("delimiter")
    private String delimiter;

    public SnapshotTableV2() {

    }

    public SnapshotTableV2(IReadableTable table, String tableName) throws IOException {
        this.tableName = tableName;
        this.signature = table.getSignature();
        this.snapshotVersion = VERSION_ID;
    }

    @Override
    public void init(IReadableTable table, TableDesc tableDesc) throws IOException {
        try {
            this.delimiter = kylinConfig.getSnapshotTableDelimiter();
            this.signature = table.getSignature();
            this.sourceTable = table;
            this.sourceTableDesc = tableDesc;
            if (kylinConfig.isSnapshotTableCompressEnabled()) {
                this.compressionCodecName = "GZIP";
            }
            int maxIndex = sourceTableDesc.getMaxColumnIndex();
            TableReader reader = sourceTable.getReader();
            logger.info("Start generate MD5.");
            rowCount = 0;
            MessageDigest digest = MessageDigest.getInstance(hashAlgorithm);
            while (reader.next()) {
                rowCount++;
                String[] rawRow = reader.getRow();
                if (rawRow.length <= maxIndex) {
                    throw new IllegalStateException("Bad hive table row, " + this.sourceTableDesc + " expect "
                            + (maxIndex + 1) + " columns, but got " + Arrays.toString(rawRow));
                }
                for (ColumnDesc column : this.sourceTableDesc.getColumns()) {
                    String cell = rawRow[column.getZeroBasedIndex()];
                    if (cell != null) {
                        if (cell.contains(standbyDelimiter)) {
                            existsStandbyDelimiter = true;
                        }
                        if (cell.contains(delimiter)) {
                            existsDelimiter = true;
                        }
                        digest.update(cell.getBytes());
                    } else {
                        existsNULL = true;
                    }
                }
            }
            if (existsStandbyDelimiter && existsDelimiter) {
                throw new RuntimeException(String.format(
                        "Source data contains current delimiter %s and standby delimiter %s. Please change property kylin.snapshot.delimiter.",
                        delimiter, standbyDelimiter));
            } else if (existsDelimiter) {
                delimiter = standbyDelimiter;
            }
            logger.info("Current delimiter is set to {}.", delimiter);

            setRowCount(rowCount);
            setMD5(Hex.encodeHexString(digest.digest()));
        } catch (Throwable e) {
            ThrowableUtils.logError(logger, "Init snapshot fail.", e);
            throw new RuntimeException(e);
        }
    }

    private void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public TableReader getReader() {
        return new TableReader() {
            int i = -1;

            @Override
            public boolean next() throws IOException {
                try {
                    i++;
                    return i < contentsCache.size();
                } catch (NullPointerException npe) {
                    throw new RuntimeException(
                            "This snapshot table does not init, pls use SnapshotManager.getSnapshotTable() api.", npe);
                }
            }

            @Override
            public String[] getRow() {
                return contentsCache.get(i);
            }

            @Override
            public void close() throws IOException {

            }
        };
    }

    @Override
    public void writeData(DataOutputStream out) throws IOException {
        if (kylinConfig.isSnapshotTableCompressEnabled()) {
            codec.setConf(new Configuration());
        }
        try (TableReader reader = sourceTable.getReader();
                OutputStream outputStream = kylinConfig.isSnapshotTableCompressEnabled() ? codec.createOutputStream(out)
                        : out) {
            logger.info("Start write snapshot table.");
            String[] row = new String[sourceTableDesc.getColumns().length];
            int maxIndex = sourceTableDesc.getMaxColumnIndex();
            while (reader.next()) {
                String[] rawRow = reader.getRow();
                if (rawRow.length <= maxIndex) {
                    throw new IllegalStateException("Bad hive table row, " + sourceTableDesc + " expect "
                            + (maxIndex + 1) + " columns, but got " + Arrays.toString(rawRow));
                }
                int i = 0;
                for (ColumnDesc column : sourceTableDesc.getColumns()) {
                    String cell = rawRow[column.getZeroBasedIndex()];
                    if (cell == null) {
                        cell = NULL_STR;
                    }
                    row[i++] = cell;
                }
                byte[] rowBytes = rowFormat(row).getBytes();
                outputStream.write(rowBytes, 0, rowBytes.length);
            }
        } catch (Throwable e) {
            ThrowableUtils.logError(logger, "Write snapshot table failed.", e);
            throw e;
        }
        logger.info("Write snapshot table done.");
    }

    private String rowFormat(String[] row) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < row.length; i++) {
            builder.append(row[i]);
            if (i < (row.length - 1)) {
                builder.append(delimiter);
            }
        }
        return builder.append("\n").toString();
    }

    private String[] rowDeFormat(String row) {
        return row.split(delimiter, -1);
    }

    @Override
    public void readData(DataInputStream in) throws IOException {
        if (!compressionCodecName.equals("UNCOMPRESSED") && codec.getConf() == null) {
            codec.setConf(new Configuration());
        }
        try (InputStream inputStream = compressionCodecName.equals("UNCOMPRESSED") ? in : codec.createInputStream(in);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            contentsCache = new ArrayList<>(rowCount);
            logger.info("Start read data");
            if (existsNULL) {
                handleNullInRead(reader);
            } else {
                handleNotNullInRead(reader);
            }
            logger.info("Read data done, total read {} rows.", contentsCache.size());
        } catch (Throwable e) {
            ThrowableUtils.logError(logger, "Read data failed.", e);
            throw e;
        }
    }

    private void handleNullInRead(BufferedReader reader) throws IOException {
        String row = reader.readLine();
        while (row != null) {
            String[] cells = rowDeFormat(row);
            for (int i = 0; i < cells.length; i++) {
                if (NULL_STR.equals(cells[i])) {
                    cells[i] = null;
                }
            }
            contentsCache.add(cells);
            row = reader.readLine();
        }
    }

    private void handleNotNullInRead(BufferedReader reader) throws IOException {
        String row = reader.readLine();
        while (row != null) {
            contentsCache.add(rowDeFormat(row));
            row = reader.readLine();
        }
    }

    @Override
    public long getRowCount() {
        return contentsCache.size();
    }

    @Override
    public int hashCode() {
        return md5.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if ((o instanceof SnapshotTableV2) == false)
            return false;
        SnapshotTableV2 that = (SnapshotTableV2) o;
        if (md5.equals(that.getMd5())) {
            return true;
        } else {
            return false;
        }

    }

    public void setMD5(String md5) {
        this.md5 = md5;
    }

    public String getMd5() {
        return md5;
    }

    public String getDelimiter() {
        return delimiter;
    }

}
