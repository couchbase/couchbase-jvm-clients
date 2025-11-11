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
import com.couchbase.client.core.api.kv.CoreExpiry;
import com.couchbase.client.java.codec.Transcoder;
import java.time.Duration;
import java.time.Instant;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * Operations controlling a transactional insert.
 */
@Stability.Volatile
public class TransactionInsertOptions {
    private @Nullable Transcoder transcoder;
    private @Nullable CoreExpiry expiry;

    private TransactionInsertOptions() {
    }

    public static TransactionInsertOptions transactionInsertOptions() {
        return new TransactionInsertOptions();
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
    public TransactionInsertOptions transcoder(Transcoder transcoder) {
        this.transcoder = transcoder;
        return this;
    }

    /**
     * Sets the expiry for the document. By default the document will never expire.
     *
     * @param expiry the duration after which the document will expire (zero duration means never expire).
     * @return this options class for chaining purposes.
     */
    public TransactionInsertOptions expiry(Duration expiry) {
        this.expiry = CoreExpiry.of(notNull(expiry, "expiry"));
        return this;
    }

    /**
     * Sets the expiry for the document. By default the document will never expire.
     *
     * @param expiry the point in time when the document will expire (epoch second zero means never expire).
     * @return this options class for chaining purposes.
     */
    public TransactionInsertOptions expiry(Instant expiry) {
        this.expiry = CoreExpiry.of(notNull(expiry, "expiry"));
        return this;
    }

    @Stability.Internal
    public TransactionInsertOptions.Built build() {
        return new TransactionInsertOptions.Built();
    }

    @Stability.Internal
    public class Built {
        Built() {
        }

        public @Nullable Transcoder transcoder() {
            return transcoder;
        }

        public CoreExpiry expiry() {
            return expiry;
        }
    }
}
