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

import com.couchbase.InternalPerformerFailure;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.transaction.cleanup.CleanerFactory;
import com.couchbase.client.core.transaction.cleanup.CleanerMockFactory;
import com.couchbase.client.core.transaction.cleanup.ClientRecordFactory;
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryProfile;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.transactions.TransactionKeyspace;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.config.TransactionsCleanupConfig;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
import com.couchbase.client.protocol.shared.ClusterConnectionCreateRequest;
import com.couchbase.client.protocol.transactions.CommandQuery;
import com.couchbase.client.protocol.shared.Durability;
import com.couchbase.client.protocol.transactions.TransactionCreateRequest;
import com.couchbase.client.protocol.transactions.TransactionQueryOptions;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class OptionsUtil {
    private OptionsUtil() {}

    @Nullable public static
    ClusterEnvironment.Builder convertClusterConfig(ClusterConnectionCreateRequest request, Supplier<ClusterConnection> getCluster) {
        ClusterEnvironment.Builder clusterEnvironment = null;

        if (request.hasClusterConfig()) {
            var cc = request.getClusterConfig();
            clusterEnvironment = ClusterEnvironment.builder();

            if (cc.getUseCustomSerializer()) {
                clusterEnvironment.jsonSerializer(new CustomSerializer());
            }

            if (request.getClusterConfig().hasTransactionsConfig()) {
                var tc = request.getClusterConfig().getTransactionsConfig();
                var builder = TransactionsConfig.builder();

                var factory = HooksUtil.configureHooks(tc.getHookList(), getCluster);
                var cleanerFactory = new CleanerMockFactory(HooksUtil.configureCleanupHooks(tc.getHookList(), getCluster));

                if (tc.hasDurability()) {
                    DurabilityLevel durabilityLevel = convertDurabilityLevel(tc.getDurability());
                    builder.durabilityLevel(durabilityLevel);
                }

                if (tc.hasCleanupConfig()) {
                    var cleanupConfig = tc.getCleanupConfig();
                    var cleanupBuilder = TransactionsCleanupConfig.builder();
                    if (cleanupConfig.hasCleanupLostAttempts()) {
                        cleanupBuilder.cleanupLostAttempts(cleanupConfig.getCleanupLostAttempts());
                    }
                    if (cleanupConfig.hasCleanupClientAttempts()) {
                        cleanupBuilder.cleanupClientAttempts(cleanupConfig.getCleanupClientAttempts());
                    }
                    if (cleanupConfig.hasCleanupWindowMillis()) {
                        cleanupBuilder.cleanupWindow(Duration.ofMillis(cleanupConfig.getCleanupWindowMillis()));
                    }
                    if (cleanupConfig.getCleanupCollectionCount() > 0) {
                        cleanupBuilder.addCollections(cleanupConfig.getCleanupCollectionList()
                                .stream()
                                .map(v -> TransactionKeyspace.create(v.getBucketName(), v.getScopeName(), v.getCollectionName()))
                                .collect(Collectors.toList()));
                    }
                    builder.cleanupConfig(cleanupBuilder);
                }

                if (tc.hasTimeoutMillis()) {
                    builder.timeout(Duration.ofMillis(tc.getTimeoutMillis()));
                }

                try {
                    // Using reflection to avoid making this internal method public
                    var method = TransactionsConfig.Builder.class.getDeclaredMethod("testFactories",
                            TransactionAttemptContextFactory.class,
                            CleanerFactory.class,
                            ClientRecordFactory.class);
                    method.setAccessible(true);
                    method.invoke(builder, factory, cleanerFactory, null);
                }
                catch (Throwable err) {
                    throw new InternalPerformerFailure(new RuntimeException(err));
                }

                if (tc.hasMetadataCollection()) {
                    builder.metadataCollection(TransactionKeyspace.create(tc.getMetadataCollection().getBucketName(),
                            tc.getMetadataCollection().getScopeName(),
                            tc.getMetadataCollection().getCollectionName()));
                }

                clusterEnvironment.transactionsConfig(builder);
            }
        }

        return clusterEnvironment;
    }

    public static DurabilityLevel convertDurabilityLevel(Durability durability) {
        DurabilityLevel durabilityLevel = DurabilityLevel.MAJORITY;
        switch (durability) {
            case NONE:
                durabilityLevel = DurabilityLevel.NONE;
                break;
            case MAJORITY:
                durabilityLevel = DurabilityLevel.MAJORITY;
                break;
            case MAJORITY_AND_PERSIST_TO_ACTIVE:
                durabilityLevel = DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
                break;
            case PERSIST_TO_MAJORITY:
                durabilityLevel = DurabilityLevel.PERSIST_TO_MAJORITY;
                break;
        }
        return durabilityLevel;
    }

    public static com.couchbase.client.java.transactions.TransactionQueryOptions transactionQueryOptions(CommandQuery request) {
        com.couchbase.client.java.transactions.TransactionQueryOptions queryOptions = null;
        if (request.hasQueryOptions()) {
            queryOptions = com.couchbase.client.java.transactions.TransactionQueryOptions.queryOptions();
            TransactionQueryOptions qo = request.getQueryOptions();

            if (qo.hasScanConsistency()) {
                queryOptions.scanConsistency(QueryScanConsistency.valueOf(qo.getScanConsistency().name()));
            }

            if (qo.getRawCount() > 0) {
                qo.getRawMap().forEach(queryOptions::raw);
            }

            if (qo.hasAdhoc()) {
                queryOptions.adhoc(qo.getAdhoc());
            }

            if (qo.hasProfile()) {
                queryOptions.profile(QueryProfile.valueOf(qo.getProfile()));
            }

            if (qo.hasReadonly()) {
                queryOptions.readonly(qo.getReadonly());
            }

            if (qo.getParametersNamedCount() > 0) {
                queryOptions.parameters(JsonArray.from(qo.getParametersPositionalList()));
            }

            if (qo.getParametersNamedCount() > 0) {
                queryOptions.parameters(JsonObject.from(qo.getParametersNamedMap()));
            }

            if (qo.hasFlexIndex()) {
                queryOptions.flexIndex(qo.getFlexIndex());
            }

            if (qo.hasPipelineCap()) {
                queryOptions.pipelineCap(qo.getPipelineCap());
            }

            if (qo.hasPipelineBatch()) {
                queryOptions.pipelineBatch(qo.getPipelineBatch());
            }

            if (qo.hasScanCap()) {
                queryOptions.scanCap(qo.getScanCap());
            }

            if (qo.hasScanWaitMillis()) {
                queryOptions.scanWait(Duration.ofMillis(qo.getScanWaitMillis()));
            }
        }
        return queryOptions;
    }

    public static @Nullable TransactionOptions makeTransactionOptions(ClusterConnection connection, TransactionCreateRequest req) {
        TransactionOptions ptcb = null;
        if (req.hasOptions()) {
            var to = req.getOptions();
            ptcb = TransactionOptions.transactionOptions();

            if (to.hasDurability()) {
                ptcb.durabilityLevel(convertDurabilityLevel(to.getDurability()));
            }

            if (to.hasMetadataCollection()) {
                com.couchbase.client.protocol.shared.Collection mc = to.getMetadataCollection();
                ptcb.metadataCollection(connection.cluster().bucket(mc.getBucketName())
                        .scope(mc.getScopeName())
                        .collection(mc.getCollectionName()));
            }

            if (to.hasTimeoutMillis()) {
                ptcb.timeout(Duration.ofMillis(to.getTimeoutMillis()));
            }

            if (to.getHookCount() > 0) {
                var factory = HooksUtil.configureHooks(to.getHookList(), () -> connection);
                try {
                    // Using reflection to avoid making this internal method public
                    var method = TransactionOptions.class.getDeclaredMethod("testFactory", TransactionAttemptContextFactory.class);
                    method.setAccessible(true);
                    method.invoke(ptcb, factory);
                } catch (Throwable err) {
                    throw new InternalPerformerFailure(new RuntimeException(err));
                }
            }
        }
        return ptcb;
    }
}
