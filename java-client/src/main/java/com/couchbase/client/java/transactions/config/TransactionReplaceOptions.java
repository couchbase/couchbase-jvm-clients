/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.transactions.config;

import com.couchbase.client.core.annotation.SinceCouchbase;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.codec.Transcoder;
import reactor.util.annotation.Nullable;

/**
 * Operations controlling a transactional replace.
 */
@Stability.Volatile
public class TransactionReplaceOptions {
    private @Nullable Transcoder transcoder;

    private TransactionReplaceOptions() {
    }

    public static TransactionReplaceOptions transactionReplaceOptions() {
        return new TransactionReplaceOptions();
    }

    /**
     * Specify a custom {@link Transcoder} that is used to encode the content.
     * <p>
     * If not-specified, the {@link com.couchbase.client.java.env.ClusterEnvironment}'s {@link com.couchbase.client.java.codec.JsonSerializer}
     * (NOT transcoder) is used.
     * <p>
     * It is marked as being available from 7.6.2 because prior to this, only JSON documents were supported in transactions.  This release added
     * support for binary documents.
     *
     * @param transcoder the custom transcoder that should be used for encoding.
     * @return this to allow method chaining.
     */
    @SinceCouchbase("7.6.2")
    public TransactionReplaceOptions transcoder(Transcoder transcoder) {
        this.transcoder = transcoder;
        return this;
    }

    @Stability.Internal
    public TransactionReplaceOptions.Built build() {
        return new TransactionReplaceOptions.Built();
    }

    @Stability.Internal
    public class Built {
        Built() {
        }

        public Transcoder transcoder() {
            return transcoder;
        }
    }
}
