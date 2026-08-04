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
import com.couchbase.client.java.transactions.config.TransactionGetOptions;
import com.couchbase.client.java.transactions.config.TransactionGetReplicaFromPreferredServerGroupOptions;
import com.couchbase.client.java.transactions.config.TransactionInsertOptions;
import com.couchbase.client.java.transactions.config.TransactionReplaceOptions;
import com.couchbase.client.protocol.transactions.CommandGet;
import com.couchbase.client.protocol.transactions.CommandGetReplicaFromPreferredServerGroup;
import com.couchbase.client.protocol.transactions.CommandInsert;
import com.couchbase.client.protocol.transactions.CommandReplace;
import com.couchbase.client.protocol.transactions.Get;
import com.couchbase.client.protocol.transactions.Insert;
import com.couchbase.client.protocol.transactions.Replace;

import java.time.Duration;
import java.time.Instant;

public class TransactionOptionsUtil {
    private TransactionOptionsUtil() { }

    public static TransactionReplaceOptions transactionReplaceOptions(CommandReplace request) {
        TransactionReplaceOptions options = null;
        if (request.hasOptions()) {
            options = TransactionReplaceOptions.transactionReplaceOptions();
            var opts = request.getOptions();
            if (opts.hasTranscoder()) {
                options = options.transcoder(JavaSdkCommandExecutor.convertTranscoder(opts.getTranscoder()));
            }
            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasAbsoluteEpochSecs()) {
                    options = options.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                }
                else if (opts.getExpiry().hasRelativeSecs()) {
                    options = options.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                }
                else {
                    throw new RuntimeException("Invalid expiry: " + opts.getExpiry());
                }
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
            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasAbsoluteEpochSecs()) {
                    options = options.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                }
                else if (opts.getExpiry().hasRelativeSecs()) {
                    options = options.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                }
                else {
                    throw new RuntimeException("Invalid expiry: " + opts.getExpiry());
                }
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
            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasAbsoluteEpochSecs()) {
                    options = options.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                }
                else if (opts.getExpiry().hasRelativeSecs()) {
                    options = options.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                }
                else {
                    throw new RuntimeException("Invalid expiry: " + opts.getExpiry());
                }
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
            if (opts.hasExpiry()) {
                if (opts.getExpiry().hasAbsoluteEpochSecs()) {
                    options = options.expiry(Instant.ofEpochSecond(opts.getExpiry().getAbsoluteEpochSecs()));
                }
                else if (opts.getExpiry().hasRelativeSecs()) {
                    options = options.expiry(Duration.ofSeconds(opts.getExpiry().getRelativeSecs()));
                }
                else {
                    throw new RuntimeException("Invalid expiry: " + opts.getExpiry());
                }
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

    public static TransactionGetReplicaFromPreferredServerGroupOptions transactionGetReplicaFromPreferredServerGroupOptions(CommandGetReplicaFromPreferredServerGroup request) {
        TransactionGetReplicaFromPreferredServerGroupOptions options = null;
        if (request.hasOptions()) {
            options = TransactionGetReplicaFromPreferredServerGroupOptions.transactionGetReplicaFromPreferredServerGroupOptions();
            var opts = request.getOptions();
            if (opts.hasTranscoder()) {
                options = options.transcoder(JavaSdkCommandExecutor.convertTranscoder(opts.getTranscoder()));
            }
        }
        return options;
    }
}
