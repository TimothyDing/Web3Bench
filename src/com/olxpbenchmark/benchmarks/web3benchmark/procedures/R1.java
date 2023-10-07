/*
 * Copyright 2023 by Web3Bench Project
 * This work was based on the OLxPBench Project

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *  http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.

 */


package com.olxpbenchmark.benchmarks.web3benchmark.procedures;

import com.olxpbenchmark.api.SQLStmt;
import com.olxpbenchmark.benchmarks.web3benchmark.WEB3Config;
import com.olxpbenchmark.benchmarks.web3benchmark.WEB3Constants;
import com.olxpbenchmark.benchmarks.web3benchmark.WEB3Util;
import com.olxpbenchmark.benchmarks.web3benchmark.WEB3Worker;
import com.olxpbenchmark.benchmarks.web3benchmark.procedures.WEB3Procedure;
import com.olxpbenchmark.distributions.ZipfianGenerator;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

public class R1 extends WEB3Procedure {

    private static final Logger LOG = Logger.getLogger(R1.class);

    // Equality on hash in transaction table
    public SQLStmt query_stmtSQL = new SQLStmt(
            "select "
                    +     "to_address, from_address "
                    + "from "
                    +     "transactions "
                    + "where "
                    +     "hash = ? "
    );

    private PreparedStatement query_stmt = null;

    public ResultSet run(Connection conn, Random gen,  WEB3Worker w, int startNumber, int upperLimit, int numScale, String nodeid) throws SQLException {
        boolean trace = LOG.isTraceEnabled();

        // initializing all prepared statements
        query_stmt = this.getPreparedStatement(conn, query_stmtSQL);

        String hash = WEB3Util.convertToTxnHashString(WEB3Util.randomNumber(1, WEB3Config.configTransactionsCount * numScale, gen));
        
        query_stmt.setString(1, hash);
        if (trace) LOG.trace("query_stmt R1 START");
        ResultSet rs = query_stmt.executeQuery();
        if (trace) LOG.trace("query_stmt R1 END");
        
        if (trace) {
            String rs_to_address = null;
            String rs_from_address = null;
            if (!rs.next()) {
                String msg = String.format("Failed to get transactions [hash=%s]", hash);
                if (trace) LOG.warn(msg);
            }
            else {
                rs_to_address = rs.getString("to_address");
                rs_from_address = rs.getString("from_address");
                // commit the transaction
                conn.commit();
            }
            
            LOG.info(query_stmt.toString());
            LOG.info(String.format("R1: to_address=%s, from_address=%s", rs_to_address, rs_from_address));
        }

        rs.close();

        return null;
    }
}


