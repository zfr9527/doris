// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.persist;

import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecoverInfo implements Writable, GsonPostProcessable {
    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "newDbName")
    private String newDbName;
    @SerializedName(value = "tableId")
    private long tableId;
    @SerializedName(value = "tableName")
    private String tableName;                        /// added for table name.
    @SerializedName(value = "newTableName")
    private String newTableName;
    @SerializedName(value = "partitionId")
    private long partitionId;
    @SerializedName(value = "partitionName")
    private String partitionName;
    @SerializedName(value = "newPartitionName")
    private String newPartitionName;

    private RecoverInfo() {
        // for persist
    }

    public RecoverInfo(long dbId, long tableId, long partitionId, String newDbName, String tableName,
                        String newTableName, String partitionName, String newPartitionName) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.tableName = tableName;
        this.partitionId = partitionId;
        this.newDbName = newDbName;
        this.newTableName = newTableName;
        this.partitionName = partitionName;
        this.newPartitionName = newPartitionName;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public String getNewDbName() {
        return newDbName;
    }

    public String getNewTableName() {
        return newTableName;
    }

    public String getNewPartitionName() {
        return newPartitionName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static RecoverInfo read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), RecoverInfo.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        newDbName = ClusterNamespace.getNameFromFullName(newDbName);
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    public static RecoverInfo fromJson(String json) {
        return GsonUtils.GSON.fromJson(json, RecoverInfo.class);
    }
}
