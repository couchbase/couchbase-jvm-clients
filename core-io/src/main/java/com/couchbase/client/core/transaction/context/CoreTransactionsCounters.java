/*
 * Copyright (c) 2020 Couchbase, Inc.
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
package com.couchbase.client.core.transaction.context;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.Counter;
import com.couchbase.client.core.cnc.Meter;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.topology.ClusterIdentifier;
import com.couchbase.client.core.topology.ClusterIdentifierUtil;
import com.couchbase.client.core.util.CbCollections;
import reactor.util.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.core.cnc.TracingIdentifiers.METER_TRANSACTION_ATTEMPTS;
import static com.couchbase.client.core.cnc.TracingIdentifiers.METER_TRANSACTION_TOTAL;
import static com.couchbase.client.core.cnc.TracingIdentifiers.SERVICE_TRANSACTIONS;

@Stability.Internal
public class CoreTransactionsCounters {
    @Stability.Internal
    public static class TransactionMetricIdentifier {

        private final @Nullable String clusterName;
        private final @Nullable String clusterUuid;

        TransactionMetricIdentifier(@Nullable ClusterIdentifier clusterIdent) {
            clusterName = clusterIdent == null ? null : clusterIdent.clusterName();
            clusterUuid = clusterIdent == null ? null : clusterIdent.clusterUuid();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TransactionMetricIdentifier that = (TransactionMetricIdentifier) o;
            return Objects.equals(clusterName, that.clusterName)
                    && Objects.equals(clusterUuid, that.clusterUuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clusterName, clusterUuid);
        }
    }

    private final Map<TransactionMetricIdentifier, Counter> transactionsMetrics = new ConcurrentHashMap<>();
    private final Map<TransactionMetricIdentifier, Counter> attemptMetrics = new ConcurrentHashMap<>();
    private final Core core;
    private final Meter meter;

    public CoreTransactionsCounters(Core core, Meter meter) {
        this.core = core;
        this.meter = meter;
    }

    public Counter attempts() {
        return genericCounter(METER_TRANSACTION_ATTEMPTS, attemptMetrics);
    }

    public Counter transactions() {
        return genericCounter(METER_TRANSACTION_TOTAL, transactionsMetrics);
    }

    private Counter genericCounter(String name, Map<TransactionMetricIdentifier, Counter> metricsMap) {
        ClusterConfig config = core.configurationProvider().config();
        ClusterIdentifier clusterIdent = ClusterIdentifierUtil.fromConfig(config);
        return metricsMap.computeIfAbsent(new TransactionMetricIdentifier(clusterIdent), id -> {
            HashMap<String, String> tags = new HashMap<>();
            tags.put(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);
            if (id.clusterName != null) {
                tags.put(TracingIdentifiers.ATTR_CLUSTER_NAME, id.clusterName);
            }
            if (id.clusterUuid != null) {
                tags.put(TracingIdentifiers.ATTR_CLUSTER_UUID, id.clusterUuid);
            }
            return meter.counter(name, tags);
        });
    }
}
