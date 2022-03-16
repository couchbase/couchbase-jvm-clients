/*
 * Copyright (c) 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.transactions;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.util.CbStrings;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.util.Validators.notNull;

/**
 * A keyspace represents a triple of bucket, scope and collection.
 */
@Stability.Internal
public class TransactionKeyspace {

    private final String bucket;
    private final String scope;
    private final String collection;

    private TransactionKeyspace(final String bucket, @Nullable final String scope, @Nullable final String collection) {
        this.bucket = notNull(bucket, "bucket");
        this.scope = CbStrings.isNullOrEmpty(scope) ? CollectionIdentifier.DEFAULT_SCOPE : scope;
        this.collection = CbStrings.isNullOrEmpty(collection) ? CollectionIdentifier.DEFAULT_COLLECTION : collection;
    }

    /**
     * Creates a keyspace with a bucket name and default scope and default collection.
     *
     * @param bucket the name of the bucket.
     * @return the created keyspace for the eventing function.
     */
    public static TransactionKeyspace create(final String bucket) {
        return new TransactionKeyspace(bucket, null, null);
    }

    /**
     * Creates a keyspace with a bucket name, collection name and default scope.
     *
     * @param bucket the name of the bucket.
     * @param collection the name of the collection.
     * @return the created keyspace for the eventing function.
     */
    public static TransactionKeyspace create(final String bucket, final String collection) {
        return new TransactionKeyspace(bucket, null, collection);
    }

    /**
     * Creates a keyspace with bucket name, scope name and collection name.
     *
     * @param bucket the name of the bucket.
     * @param scope the name of the scope.
     * @param collection the name of the collection.
     * @return the created keyspace for the eventing function.
     */
    public static TransactionKeyspace create(final String bucket, final String scope, final String collection) {
        return new TransactionKeyspace(bucket, scope, collection);
    }

    /**
     * The name of the bucket.
     */
    public String bucket() {
        return bucket;
    }

    /**
     * The name of the scope.
     */
    public String scope() {
        return scope;
    }

    /**
     * The name of the collection.
     */
    public String collection() {
        return collection;
    }

    @Override
    public String toString() {
        return "TransactionKeyspace{" +
                "bucket='" + bucket + '\'' +
                ", scope='" + scope + '\'' +
                ", collection='" + collection + '\'' +
                '}';
    }
}
