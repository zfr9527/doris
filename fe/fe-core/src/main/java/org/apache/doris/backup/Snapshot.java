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

package org.apache.doris.backup;

import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

public class Snapshot {
    @SerializedName(value = "label")
    private String label = null;

    @SerializedName(value = "meta")
    private byte[] meta = null;

    @SerializedName(value = "jobInfo")
    private byte[] jobInfo = null;

    @SerializedName(value = "expired_at")
    private long expiredAt = 0;

    @SerializedName(value = "commitSeq")
    private long commitSeq = 0;

    public Snapshot() {
    }

    public Snapshot(String label, byte[] meta, byte[] jobInfo, long expiredAt, long commitSeq) {
        this.label = label;
        this.meta = meta;
        this.jobInfo = jobInfo;
        this.expiredAt = expiredAt;
        this.commitSeq = commitSeq;
    }

    public byte[] getMeta() {
        return meta;
    }

    public byte[] getJobInfo() {
        return jobInfo;
    }

    public long getExpiredAt() {
        return expiredAt;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > expiredAt;
    }

    public long getCommitSeq() {
        return commitSeq;
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public String toString() {
        return "Snapshot{"
                + "label='" + label + '\''
                + ", meta=" + meta
                + ", jobInfo=" + jobInfo
                + ", expiredAt=" + expiredAt
                + ", commitSeq=" + commitSeq
                + '}';
    }
}
