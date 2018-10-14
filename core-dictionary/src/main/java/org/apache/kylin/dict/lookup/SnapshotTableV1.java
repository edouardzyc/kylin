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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.exceptions.TooBigDictionaryException;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.StringBytesConverter;
import org.apache.kylin.dict.TrieDictionary;
import org.apache.kylin.dict.TrieDictionaryBuilder;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class SnapshotTableV1 extends SnapshotTable {
    @JsonProperty("useDictionary")
    protected boolean useDictionary;

    protected ArrayList<int[]> rowIndices;
    protected Dictionary<String> dict;

    public static String VERSION_ID = "V1";

    public SnapshotTableV1() {

    }

    public SnapshotTableV1(IReadableTable table, String tableName) throws IOException {
        this.tableName = tableName;
        this.signature = table.getSignature();
        this.useDictionary = true;
        this.snapshotVersion = VERSION_ID;
    }

    @Override
    public void init(IReadableTable table, TableDesc tableDesc) throws IOException {
        this.signature = table.getSignature();

        int maxIndex = tableDesc.getMaxColumnIndex();

        TrieDictionaryBuilder<String> b = new TrieDictionaryBuilder<String>(new StringBytesConverter());

        TableReader reader = table.getReader();
        try {
            while (reader.next()) {
                String[] row = reader.getRow();
                if (row.length <= maxIndex) {
                    throw new IllegalStateException("Bad hive table row, " + tableDesc + " expect " + (maxIndex + 1)
                            + " columns, but got " + Arrays.toString(row));
                }
                for (ColumnDesc column : tableDesc.getColumns()) {
                    String cell = row[column.getZeroBasedIndex()];
                    if (cell != null)
                        b.addValue(cell);
                }
            }
        } finally {
            IOUtils.closeQuietly(reader);
        }

        try {
            this.dict = b.build(0);
        } catch (Exception e) {
            if (e instanceof TooBigDictionaryException)
                throw new TooBigDictionaryException("The dictionary of '" + tableDesc + "' is bigger then 2GB", e);
            else
                throw e;
        }

        ArrayList<int[]> allRowIndices = new ArrayList<int[]>();
        reader = table.getReader();
        try {
            while (reader.next()) {
                String[] row = reader.getRow();
                int[] rowIndex = new int[tableDesc.getColumnCount()];
                for (ColumnDesc column : tableDesc.getColumns()) {
                    rowIndex[column.getZeroBasedIndex()] = dict.getIdFromValue(row[column.getZeroBasedIndex()]);
                }
                allRowIndices.add(rowIndex);
            }
        } finally {
            IOUtils.closeQuietly(reader);
        }

        this.rowIndices = allRowIndices;
    }

    @Override
    public TableReader getReader() throws IOException {
        return new TableReader() {

            int i = -1;

            @Override
            public boolean next() throws IOException {
                i++;
                return i < rowIndices.size();
            }

            @Override
            public String[] getRow() {
                int[] rowIndex = rowIndices.get(i);
                String[] row = new String[rowIndex.length];
                for (int x = 0; x < row.length; x++) {
                    row[x] = dict.getValueFromId(rowIndex[x]);
                }
                return row;
            }

            @Override
            public void close() throws IOException {
            }
        };
    }

    public ObjectTableReader toObjectRead(Map<Integer, Dictionary<String>> dictIndex, Set<Integer> dateIndex) {
        return new ObjectTableReader(dictIndex, dateIndex);
    }

    public class ObjectTableReader {
        Map<Integer, Dictionary<String>> dictIndex;
        Set<Integer> dateIndex;
        int i = -1;

        public ObjectTableReader(Map<Integer, Dictionary<String>> dictIndex, Set<Integer> dateIndex) {
            this.dictIndex = dictIndex;
            this.dateIndex = dateIndex;
        }

        public boolean next() throws IOException {
            i++;
            return i < rowIndices.size();
        }

        public Object[] getRow() {
            int[] rowIndex = rowIndices.get(i);
            Object[] row = new Object[rowIndex.length];
            for (int x = 0; x < row.length; x++) {
                Object valueFromId = dict.getValueFromId(rowIndex[x]);

                if (dictIndex.containsKey(x)) {
                    Object idFromValue = dictIndex.get(x).getIdFromValue(valueFromId.toString());
                    row[x] = idFromValue;
                } else if (dateIndex.contains(x)) {
                    row[x] = (Object) ((DateFormat.stringToMillis(valueFromId.toString())) / 1000);
                } else {
                    row[x] = valueFromId;
                }
            }
            return row;
        }

        public void close() throws IOException {
        }
    }

    @Override
    public void writeData(DataOutputStream out) throws IOException {
        out.writeInt(rowIndices.size());
        if (rowIndices.size() > 0) {
            int n = rowIndices.get(0).length;
            out.writeInt(n);

            if (this.useDictionary == true) {
                dict.write(out);
                for (int i = 0; i < rowIndices.size(); i++) {
                    int[] row = rowIndices.get(i);
                    for (int j = 0; j < n; j++) {
                        out.writeInt(row[j]);
                    }
                }

            } else {
                for (int i = 0; i < rowIndices.size(); i++) {
                    int[] row = rowIndices.get(i);
                    for (int j = 0; j < n; j++) {
                        // NULL_STR is tricky, but we don't want to break the current snapshots
                        out.writeUTF(dict.getValueFromId(row[j]) == null ? NULL_STR : dict.getValueFromId(row[j]));
                    }
                }
            }
        }
    }

    /**
     * a naive implementation
     *
     * @return
     */
    @Override
    public int hashCode() {
        int[] parts = new int[this.rowIndices.size()];
        for (int i = 0; i < parts.length; ++i)
            parts[i] = Arrays.hashCode(this.rowIndices.get(i));
        return Arrays.hashCode(parts);
    }

    @Override
    public boolean equals(Object o) {
        if ((o instanceof SnapshotTableV1) == false)
            return false;
        SnapshotTableV1 that = (SnapshotTableV1) o;

        if (this.dict.equals(that.dict) == false)
            return false;

        //compare row by row
        if (this.rowIndices.size() != that.rowIndices.size())
            return false;
        for (int i = 0; i < this.rowIndices.size(); ++i) {
            if (!ArrayUtils.isEquals(this.rowIndices.get(i), that.rowIndices.get(i)))
                return false;
        }

        return true;
    }

    @Override
    public void readData(DataInputStream in) throws IOException {
        int rowNum = in.readInt();
        if (rowNum > 0) {
            int n = in.readInt();
            rowIndices = new ArrayList<int[]>(rowNum);

            if (this.useDictionary == true) {
                this.dict = new TrieDictionary<String>();
                dict.readFields(in);

                for (int i = 0; i < rowNum; i++) {
                    int[] row = new int[n];
                    this.rowIndices.add(row);
                    for (int j = 0; j < n; j++) {
                        row[j] = in.readInt();
                    }
                }
            } else {
                List<String[]> rows = new ArrayList<String[]>(rowNum);
                TrieDictionaryBuilder<String> b = new TrieDictionaryBuilder<String>(new StringBytesConverter());

                for (int i = 0; i < rowNum; i++) {
                    String[] row = new String[n];
                    rows.add(row);
                    for (int j = 0; j < n; j++) {
                        row[j] = in.readUTF();
                        // NULL_STR is tricky, but we don't want to break the current snapshots
                        if (row[j].equals(NULL_STR))
                            row[j] = null;

                        b.addValue(row[j]);
                    }
                }
                this.dict = b.build(0);
                for (String[] row : rows) {
                    int[] rowIndex = new int[n];
                    for (int i = 0; i < n; i++) {
                        rowIndex[i] = dict.getIdFromValue(row[i]);
                    }
                    this.rowIndices.add(rowIndex);
                }
            }
        } else {
            rowIndices = new ArrayList<int[]>();
            dict = new TrieDictionary<String>();
        }
    }

    @Override
    public long getRowCount() {
        return rowIndices.size();
    }

}
