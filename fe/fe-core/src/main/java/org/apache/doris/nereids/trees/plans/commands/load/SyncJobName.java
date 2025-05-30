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

package org.apache.doris.nereids.trees.plans.commands.load;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

/**
 * SyncJobName
 */
public class SyncJobName {
    private String jobName;
    private String dbName;

    public SyncJobName(String dbName, String jobName) {
        this.dbName = dbName;
        this.jobName = jobName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getName() {
        return jobName;
    }

    /**
     * analyze
     */
    public void analyze(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName)) {
            if (Strings.isNullOrEmpty(ctx.getDatabase())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            dbName = ctx.getDatabase();
        }
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("`").append(dbName).append("`.`").append(jobName).append("`");
        return sb.toString();
    }
}
