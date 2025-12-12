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
import com.couchbase.client.core.cnc.metrics.CounterName;
import com.couchbase.client.core.cnc.metrics.WrappedCounter;
import com.couchbase.client.core.cnc.metrics.ResponseMetricIdentifier;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.topology.ClusterIdentifier;
import com.couchbase.client.core.topology.ClusterIdentifierUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.core.cnc.TracingIdentifiers.*;

@Stability.Internal
public class CoreTransactionsCounters {
    private final Map<ResponseMetricIdentifier, WrappedCounter> transactionsMetrics = new ConcurrentHashMap<>();
    private final Map<ResponseMetricIdentifier, WrappedCounter> attemptMetrics = new ConcurrentHashMap<>();
    private final Core core;

    public CoreTransactionsCounters(Core core) {
        this.core = core;
    }

    public WrappedCounter attempts() {
        return genericCounter(CounterName.TRANSACTIONS_ATTEMPT_COUNTER, attemptMetrics);
    }

    public WrappedCounter transactions() {
        return genericCounter(CounterName.TRANSACTIONS_COUNTER, transactionsMetrics);
    }

    private WrappedCounter genericCounter(CounterName name, Map<ResponseMetricIdentifier, WrappedCounter> metricsMap) {
        ClusterConfig config = core.configurationProvider().config();
        ClusterIdentifier clusterIdent = ClusterIdentifierUtil.fromConfig(config);
        boolean isDefaultLoggingMeter = core.coreResources().meter().isDefaultLoggingMeter();
        ResponseMetricIdentifier rmi = new ResponseMetricIdentifier(SERVICE_TRANSACTIONS,
                TRANSACTION_OP,
                // Transactions are not associated with any one collection - they can span
                null, null, null,
                // Including the failure cause is not currently supported
                null,
                clusterIdent,
                isDefaultLoggingMeter);
        return metricsMap.computeIfAbsent(rmi, id -> core.coreResources().meter().counter(name, id));
    }
}
