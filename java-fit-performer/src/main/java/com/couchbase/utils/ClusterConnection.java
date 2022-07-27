/*
 * Copyright 2022 Couchbase, Inc.
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
package com.couchbase.utils;


import com.couchbase.client.core.Core;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.tracing.opentelemetry.OpenTelemetryRequestTracer;
import com.couchbase.client.protocol.transactions.DocId;
import io.opentelemetry.sdk.trace.SdkTracerProvider;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;


public class ClusterConnection {
    private final Cluster cluster;
    @Nullable private final ClusterEnvironment config;
    public final String username;

    public ClusterConnection(String hostname,
                             String username,
                             String password,
                             Optional<SdkTracerProvider> tracerProvider,
                             @Nullable ClusterEnvironment.Builder config)  {
        this.username = username;

        if (tracerProvider.isPresent()) {
            if (config == null) {
                config = ClusterEnvironment.builder();
            }
            config.requestTracer(OpenTelemetryRequestTracer.wrap(tracerProvider.get()));
        }

        var co = ClusterOptions.clusterOptions(username, password);
        if (config != null) {
            this.config = config.build();
            co.environment(this.config);
        }
        else {
            this.config = null;
        }

        this.cluster = Cluster.connect(hostname, co);
        cluster.waitUntilReady(Duration.ofSeconds(30));
    }

    public Cluster cluster(){
        return cluster;
    }

    public Core core() {
        return cluster.core();
    }

    public Collection collection(DocId docId) {
        return cluster.bucket(docId.getBucketName())
                .scope(docId.getScopeName())
                .collection(docId.getCollectionName());
    }

    public void close() {
        cluster.disconnect();
        if (config != null) {
            config.shutdown();
        }
    }

    public void waitUntilReady(CollectionIdentifier collection) {
        var bucket = cluster.bucket(collection.bucket());
        bucket.waitUntilReady(Duration.ofSeconds(10));
    }
}
