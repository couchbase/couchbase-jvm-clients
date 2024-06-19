/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.twoway;

import com.couchbase.JavaSdkCommandExecutor;
// [if:3.6.2]
import com.couchbase.client.java.transactions.config.TransactionGetOptions;
import com.couchbase.client.java.transactions.config.TransactionReplaceOptions;
import com.couchbase.client.java.transactions.config.TransactionInsertOptions;
// [end]
import com.couchbase.client.protocol.transactions.CommandGet;
import com.couchbase.client.protocol.transactions.CommandInsert;
import com.couchbase.client.protocol.transactions.CommandReplace;
import com.couchbase.client.protocol.transactions.Replace;
import com.couchbase.client.protocol.transactions.Insert;
import com.couchbase.client.protocol.transactions.Get;

public class TransactionOptionsUtil {
    private TransactionOptionsUtil() { }

    // [if:3.6.2]
    public static TransactionReplaceOptions transactionReplaceOptions(CommandReplace request) {
        TransactionReplaceOptions options = null;
        if (request.hasOptions()) {
            options = TransactionReplaceOptions.transactionReplaceOptions();
            var opts = request.getOptions();
            if (opts.hasTranscoder()) {
                options = options.transcoder(JavaSdkCommandExecutor.convertTranscoder(opts.getTranscoder()));
            }
        }
        return options;
    }

    public static TransactionReplaceOptions transactionReplaceOptions(Replace request) {
        TransactionReplaceOptions options = null;
        if (request.hasOptions()) {
            options = TransactionReplaceOptions.transactionReplaceOptions();
            var opts = request.getOptions();
            if (opts.hasTranscoder()) {
                options = options.transcoder(JavaSdkCommandExecutor.convertTranscoder(opts.getTranscoder()));
            }
        }
        return options;
    }

    public static TransactionInsertOptions transactionInsertOptions(CommandInsert request) {
        TransactionInsertOptions options = null;
        if (request.hasOptions()) {
            options = TransactionInsertOptions.transactionInsertOptions();
            var opts = request.getOptions();
            if (opts.hasTranscoder()) {
                options = options.transcoder(JavaSdkCommandExecutor.convertTranscoder(opts.getTranscoder()));
            }
        }
        return options;
    }

    public static TransactionInsertOptions transactionInsertOptions(Insert request) {
        TransactionInsertOptions options = null;
        if (request.hasOptions()) {
            options = TransactionInsertOptions.transactionInsertOptions();
            var opts = request.getOptions();
            if (opts.hasTranscoder()) {
                options = options.transcoder(JavaSdkCommandExecutor.convertTranscoder(opts.getTranscoder()));
            }
        }
        return options;
    }

    public static TransactionGetOptions transactionGetOptions(CommandGet request) {
        TransactionGetOptions options = null;
        if (request.hasOptions()) {
            options = TransactionGetOptions.transactionGetOptions();
            var opts = request.getOptions();
            if (opts.hasTranscoder()) {
                options = options.transcoder(JavaSdkCommandExecutor.convertTranscoder(opts.getTranscoder()));
            }
        }
        return options;
    }

    public static TransactionGetOptions transactionGetOptions(Get request) {
        TransactionGetOptions options = null;
        if (request.hasOptions()) {
            options = TransactionGetOptions.transactionGetOptions();
            var opts = request.getOptions();
            if (opts.hasTranscoder()) {
                options = options.transcoder(JavaSdkCommandExecutor.convertTranscoder(opts.getTranscoder()));
            }
        }
        return options;
    }
    // [else]
    //? public static Object transactionReplaceOptions(CommandReplace request) { return null; }
    //? public static Object transactionReplaceOptions(Replace request) { return null; }
    //? public static Object transactionInsertOptions(CommandInsert request) { return null; }
    //? public static Object transactionInsertOptions(Insert request) { return null; }
    //? public static Object transactionGetOptions(CommandGet request) { return null; }
    //? public static Object transactionGetOptions(Get request) { return null; }
    // [end]
}
