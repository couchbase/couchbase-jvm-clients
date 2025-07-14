/*
 * Copyright (c) 2025 Couchbase, Inc.
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

package com.couchbase.client.core.cnc.metrics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.kv.KeyValueRequest;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.search.ServerSearchRequest;
import com.couchbase.client.core.topology.ClusterIdentifier;
import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Stability.Internal
public class ResponseMetricIdentifier {

    private final String serviceTracingId;
    private final String requestName;
    private final @Nullable String bucketName;
    private final @Nullable String scopeName;
    private final @Nullable String collectionName;
    private final @Nullable String exceptionSimpleName;
    private final @Nullable ClusterIdentifier clusterIdent;
    private final boolean isDefaultLoggingMeter;

    public static ResponseMetricIdentifier fromRequest(final Request<?> request,
                                                       @Nullable String exceptionSimpleName,
                                                       @Nullable ClusterIdentifier clusterIdent,
                                                       boolean isDefaultLoggingMeter) {

        String bucketName = null;
        String scopeName = null;
        String collectionName = null;

        if (request instanceof KeyValueRequest) {
            KeyValueRequest<?> kv = (KeyValueRequest<?>) request;
            bucketName = request.bucket();
            scopeName = kv.collectionIdentifier().scope().orElse(CollectionIdentifier.DEFAULT_SCOPE);
            collectionName = kv.collectionIdentifier().collection().orElse(CollectionIdentifier.DEFAULT_SCOPE);
        } else if (request instanceof QueryRequest) {
            QueryRequest query = (QueryRequest) request;
            bucketName = request.bucket();
            scopeName = query.scope();
        } else if (request instanceof ServerSearchRequest) {
            ServerSearchRequest search = (ServerSearchRequest) request;
            if (search.scope() != null) {
                bucketName = search.scope().bucketName();
                scopeName = search.scope().scopeName();
            }
        }

        return new ResponseMetricIdentifier(request.serviceTracingId(),
                request.name(),
                bucketName,
                scopeName,
                collectionName,
                exceptionSimpleName,
                clusterIdent,
                isDefaultLoggingMeter);
    }

    public ResponseMetricIdentifier(String serviceTracingId,
                                    String requestName,
                                    @Nullable String bucketName,
                                    @Nullable String scopeName,
                                    @Nullable String collectionName,
                                    @Nullable String exceptionSimpleName,
                                    @Nullable ClusterIdentifier clusterIdent,
                                    boolean isDefaultLoggingMeter) {
        this.serviceTracingId = serviceTracingId;
        this.requestName = requestName;
        this.bucketName = bucketName;
        this.scopeName = scopeName;
        this.collectionName = collectionName;
        this.exceptionSimpleName = exceptionSimpleName;
        this.clusterIdent = clusterIdent;
        this.isDefaultLoggingMeter = isDefaultLoggingMeter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResponseMetricIdentifier that = (ResponseMetricIdentifier) o;
        return serviceTracingId.equals(that.serviceTracingId)
                && Objects.equals(requestName, that.requestName)
                && Objects.equals(bucketName, that.bucketName)
                && Objects.equals(scopeName, that.scopeName)
                && Objects.equals(collectionName, that.collectionName)
                && Objects.equals(exceptionSimpleName, that.exceptionSimpleName)
                && Objects.equals(clusterIdent, that.clusterIdent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceTracingId, requestName, bucketName, scopeName, collectionName, exceptionSimpleName, clusterIdent);
    }

    public Map<String, String> tags() {
        // N.b. remember the pattern is that ResponseMetricIdentifier is created many times (on every request),
        // while this method will only be called if we don't already have a corresponding ResponseMetricIdentifier
        // entry in the map.  E.g. it would be a false optimisation to move this tags() logic into the ctor.
        Map<String, String> tags = new HashMap<>(9);
        tags.put(TracingIdentifiers.ATTR_SERVICE, serviceTracingId);
        tags.put(TracingIdentifiers.ATTR_OPERATION, requestName);

        // The LoggingMeter only uses the service and operation labels, so optimise this hot-path by skipping
        // assigning other labels.
        if (!isDefaultLoggingMeter) {
            // Crucial note for Micrometer:
            // If we are ever going to output an attribute from a given JVM run then we must always
            // output that attribute in this run.  Specifying null as an attribute value allows the OTel backend to strip it, and
            // the Micrometer backend to provide a default value.
            // See (internal to Couchbase) discussion here for full details:
            // https://issues.couchbase.com/browse/CBSE-17070?focusedId=779820&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-779820
            // If this rule is not followed, then Micrometer will silently discard some metrics.  Micrometer requires that
            // every value output under a given metric has the same set of attributes.

            tags.put(TracingIdentifiers.ATTR_NAME, bucketName);
            tags.put(TracingIdentifiers.ATTR_SCOPE, scopeName);
            tags.put(TracingIdentifiers.ATTR_COLLECTION, collectionName);

            tags.put(TracingIdentifiers.ATTR_CLUSTER_UUID, clusterIdent == null ? null : clusterIdent.clusterUuid());
            tags.put(TracingIdentifiers.ATTR_CLUSTER_NAME, clusterIdent == null ? null : clusterIdent.clusterName());

            if (exceptionSimpleName != null) {
                tags.put(TracingIdentifiers.ATTR_OUTCOME, exceptionSimpleName);
            } else {
                tags.put(TracingIdentifiers.ATTR_OUTCOME, "Success");
            }
        }

        return tags;
    }
}
