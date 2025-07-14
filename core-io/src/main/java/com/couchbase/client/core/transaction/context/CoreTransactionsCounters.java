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
import com.couchbase.client.core.cnc.metrics.LoggingMeter;
import com.couchbase.client.core.cnc.metrics.ResponseMetricIdentifier;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.topology.ClusterIdentifier;
import com.couchbase.client.core.topology.ClusterIdentifierUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.couchbase.client.core.cnc.TracingIdentifiers.*;

@Stability.Internal
public class CoreTransactionsCounters {
    private final Map<ResponseMetricIdentifier, Counter> transactionsMetrics = new ConcurrentHashMap<>();
    private final Map<ResponseMetricIdentifier, Counter> attemptMetrics = new ConcurrentHashMap<>();
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

    private Counter genericCounter(String name, Map<ResponseMetricIdentifier, Counter> metricsMap) {
        ClusterConfig config = core.configurationProvider().config();
        ClusterIdentifier clusterIdent = ClusterIdentifierUtil.fromConfig(config);
        boolean isDefaultLoggingMeter = core.context().environment().meter() instanceof LoggingMeter;
        ResponseMetricIdentifier rmi = new ResponseMetricIdentifier(SERVICE_TRANSACTIONS,
                TRANSACTION_OP,
                // Transactions are not associated with any one collection - they can span
                null, null, null,
                // Including the failure cause is not currently supported
                null,
                clusterIdent,
                isDefaultLoggingMeter);
        return metricsMap.computeIfAbsent(rmi, id -> meter.counter(name, id.tags()));
    }
}
