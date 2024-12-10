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
import com.couchbase.JavaSdkCommandExecutor;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.tracing.NoopRequestTracer;
// [if:3.5.1]
import com.couchbase.client.core.endpoint.CircuitBreakerConfig;
// [end]
import com.couchbase.client.core.env.IoConfig;
// [if:3.2.0]
import com.couchbase.client.core.env.LoggingMeterConfig;
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig;
// [end]
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryProfile;
import com.couchbase.client.java.query.QueryScanConsistency;
// [if:3.3.0]
import com.couchbase.client.core.transaction.cleanup.CleanerFactory;
import com.couchbase.client.core.transaction.cleanup.CleanerMockFactory;
import com.couchbase.client.core.transaction.cleanup.ClientRecordFactory;
import com.couchbase.client.core.transaction.support.TransactionAttemptContextFactory;
import com.couchbase.client.java.transactions.TransactionKeyspace;
import com.couchbase.client.java.transactions.config.TransactionOptions;
import com.couchbase.client.java.transactions.config.TransactionsCleanupConfig;
import com.couchbase.client.java.transactions.config.TransactionsConfig;
// [end]
import com.couchbase.client.metrics.opentelemetry.OpenTelemetryMeter;
import com.couchbase.client.protocol.observability.Attribute;
import com.couchbase.client.protocol.sdk.circuit_breaker.ServiceConfig;
import com.couchbase.client.protocol.shared.ClusterConfig;
import com.couchbase.client.protocol.shared.ClusterConnectionCreateRequest;
import com.couchbase.client.protocol.transactions.CommandQuery;
import com.couchbase.client.protocol.shared.Durability;
import com.couchbase.client.protocol.transactions.TransactionCreateRequest;
import com.couchbase.client.protocol.transactions.TransactionQueryOptions;
import com.couchbase.client.tracing.opentelemetry.OpenTelemetryRequestTracer;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.resources.ResourceBuilder;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class OptionsUtil {
    private static final Logger logger = LoggerFactory.getLogger(OptionsUtil.class);

    private OptionsUtil() {}

    public static
    ClusterEnvironment.Builder convertClusterConfig(ClusterConnectionCreateRequest request,
                                                    Supplier<ClusterConnection> getCluster,
                                                    ArrayList<Runnable> onClusterConnectionClose) {
        ClusterEnvironment.Builder clusterEnvironment = ClusterEnvironment.builder();

        if (request.hasClusterConfig()) {
            var cc = request.getClusterConfig();

            if (cc.getUseCustomSerializer()) {
                clusterEnvironment.jsonSerializer(new CustomSerializer());
            }

            // [if:3.3.0]
            if (request.getClusterConfig().hasTransactionsConfig()) {
                applyTransactionsConfig(request, getCluster, clusterEnvironment);
            }
            // [end]

            SecurityConfig.Builder secBuilder = null;
            if (cc.getUseTls()) {
                secBuilder = SecurityConfig.builder();
                secBuilder.enableTls(true);
            }

            if (cc.hasCertPath()) {
              if (secBuilder == null) secBuilder = SecurityConfig.builder();
              logger.info("Using certificate from file {}", cc.getCertPath());
              secBuilder.trustCertificate(Path.of(cc.getCertPath()));
            }

          if (cc.hasCert()) {
            if (secBuilder == null) secBuilder = SecurityConfig.builder();
            try {
              CertificateFactory cFactory = CertificateFactory.getInstance("X.509");
              var file = new ByteArrayInputStream(cc.getCert().getBytes(StandardCharsets.UTF_8));
              logger.info("Using certificate {}", cc.getCert());
              X509Certificate cert = (X509Certificate) cFactory.generateCertificate(file);

              secBuilder.trustCertificates(List.of(cert));
            }
            catch (CertificateException err) {
              throw new RuntimeException(err);
            }
          }

          if (cc.hasInsecure() && cc.getInsecure()) {
            if (secBuilder == null) secBuilder = SecurityConfig.builder();
            // Cannot use enableCertificateVerification as it was added later
            secBuilder.trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
          }

          if (secBuilder != null) {
              clusterEnvironment.securityConfig(secBuilder);
            }

            applyClusterConfig(clusterEnvironment, cc);

            if (cc.hasObservabilityConfig()) {
                applyObservabilityConfig(clusterEnvironment, cc, onClusterConnectionClose);
            }

            if (cc.hasPreferredServerGroup()) {
              // [if:3.7.4]
              clusterEnvironment.preferredServerGroup(cc.getPreferredServerGroup());
              // [end]
            }
        }

        return clusterEnvironment;
    }

    private static void applyClusterConfig(ClusterEnvironment.Builder clusterEnvironment, ClusterConfig cc) {
        IoConfig.Builder ioConfig = null;
        TimeoutConfig.Builder timeoutConfig = null;

        if (cc.hasKvConnectTimeoutSecs()) {
            if (timeoutConfig == null) timeoutConfig = TimeoutConfig.builder();
            timeoutConfig.connectTimeout(Duration.ofSeconds(cc.getKvConnectTimeoutSecs()));
        }
        if (cc.hasKvTimeoutMillis()) {
            if (timeoutConfig == null) timeoutConfig = TimeoutConfig.builder();
            timeoutConfig.kvTimeout(Duration.ofMillis(cc.getKvTimeoutMillis()));
        }
        if (cc.hasKvDurableTimeoutMillis()) {
            if (timeoutConfig == null) timeoutConfig = TimeoutConfig.builder();
            timeoutConfig.kvDurableTimeout(Duration.ofMillis(cc.getKvDurableTimeoutMillis()));
        }
        if (cc.hasViewTimeoutSecs()) {
            if (timeoutConfig == null) timeoutConfig = TimeoutConfig.builder();
            timeoutConfig.viewTimeout(Duration.ofSeconds(cc.getViewTimeoutSecs()));
        }
        if (cc.hasQueryTimeoutSecs()) {
            if (timeoutConfig == null) timeoutConfig = TimeoutConfig.builder();
            timeoutConfig.queryTimeout(Duration.ofSeconds(cc.getQueryTimeoutSecs()));
        }
        if (cc.hasAnalyticsTimeoutSecs()) {
            if (timeoutConfig == null) timeoutConfig = TimeoutConfig.builder();
            timeoutConfig.analyticsTimeout(Duration.ofSeconds(cc.getAnalyticsTimeoutSecs()));
        }
        if (cc.hasSearchTimeoutSecs()) {
            if (timeoutConfig == null) timeoutConfig = TimeoutConfig.builder();
            timeoutConfig.searchTimeout(Duration.ofSeconds(cc.getSearchTimeoutSecs()));
        }
        if (cc.hasManagementTimeoutSecs()) {
            if (timeoutConfig == null) timeoutConfig = TimeoutConfig.builder();
            timeoutConfig.managementTimeout(Duration.ofSeconds(cc.getManagementTimeoutSecs()));
        }
        if (cc.hasKvScanTimeoutSecs()) {
            if (timeoutConfig == null) timeoutConfig = TimeoutConfig.builder();
            // [if:3.4.1]
            timeoutConfig.kvScanTimeout(Duration.ofSeconds(cc.getKvScanTimeoutSecs()));
            // [end]
        }
        if (cc.hasTranscoder()) {
            clusterEnvironment.transcoder(JavaSdkCommandExecutor.convertTranscoder(cc.getTranscoder()));
        }
        if (cc.hasEnableMutationTokens()) {
            if (ioConfig == null) ioConfig = IoConfig.builder();
            ioConfig.enableMutationTokens(cc.getEnableMutationTokens());
        }
        if (cc.hasTcpKeepAliveTimeMillis()) {
            if (ioConfig == null) ioConfig = IoConfig.builder();
            ioConfig.tcpKeepAliveTime(Duration.ofMillis(cc.getTcpKeepAliveTimeMillis()));
        }
        if (cc.getForceIPV4()) {
            throw new UnsupportedOperationException();
        }
        if (cc.hasConfigPollIntervalSecs()) {
            if (ioConfig == null) ioConfig = IoConfig.builder();
            ioConfig.configPollInterval(Duration.ofSeconds(cc.getConfigPollIntervalSecs()));
        }
        if (cc.hasConfigPollFloorIntervalSecs()) {
            throw new UnsupportedOperationException();
        }
        if (cc.hasConfigIdleRedialTimeoutSecs()) {
            if (ioConfig == null) ioConfig = IoConfig.builder();
            ioConfig.configIdleRedialTimeout(Duration.ofSeconds(cc.getConfigIdleRedialTimeoutSecs()));
        }
        if (cc.hasNumKvConnections()) {
            if (ioConfig == null) ioConfig = IoConfig.builder();
            ioConfig.numKvConnections(cc.getNumKvConnections());
        }
        if (cc.hasMaxHttpConnections()) {
            if (ioConfig == null) ioConfig = IoConfig.builder();
            ioConfig.maxHttpConnections(cc.getMaxHttpConnections());
        }
        if (cc.hasIdleHttpConnectionTimeoutSecs()) {
            if (ioConfig == null) ioConfig = IoConfig.builder();
            ioConfig.idleHttpConnectionTimeout(Duration.ofSeconds(cc.getIdleHttpConnectionTimeoutSecs()));
        }
        // [if:3.5.1]
        if (cc.hasCircuitBreakerConfig()) {
            if (ioConfig == null) ioConfig = IoConfig.builder();
            var cbcc = cc.getCircuitBreakerConfig();
            if (cbcc.hasKv()) {
                ioConfig.kvCircuitBreakerConfig(cb -> applyCircuitBreakerConfig(cbcc.getKv(), cb));
            }
            if (cbcc.hasQuery()) {
                ioConfig.queryCircuitBreakerConfig(cb -> applyCircuitBreakerConfig(cbcc.getQuery(), cb));
            }
            if (cbcc.hasView()) {
                ioConfig.viewCircuitBreakerConfig(cb -> applyCircuitBreakerConfig(cbcc.getView(), cb));
            }
            if (cbcc.hasSearch()) {
                ioConfig.searchCircuitBreakerConfig(cb -> applyCircuitBreakerConfig(cbcc.getSearch(), cb));
            }
            if (cbcc.hasAnalytics()) {
                ioConfig.analyticsCircuitBreakerConfig(cb -> applyCircuitBreakerConfig(cbcc.getAnalytics(), cb));
            }
            if (cbcc.hasManager()) {
                ioConfig.managerCircuitBreakerConfig(cb -> applyCircuitBreakerConfig(cbcc.getManager(), cb));
            }
            if (cbcc.hasEventing()) {
                ioConfig.eventingCircuitBreakerConfig(cb -> applyCircuitBreakerConfig(cbcc.getEventing(), cb));
            }
            if (cbcc.hasBackup()) {
                ioConfig.backupCircuitBreakerConfig(cb -> applyCircuitBreakerConfig(cbcc.getBackup(), cb));
            }
        }
        // [end]

        if (ioConfig != null) {
            clusterEnvironment.ioConfig(ioConfig);
        }
        if (timeoutConfig != null) {
            clusterEnvironment.timeoutConfig(timeoutConfig);
        }
    }

    // [if:3.5.1]
    private static void applyCircuitBreakerConfig(ServiceConfig cbc, CircuitBreakerConfig.Builder cb) {
        if (cbc.hasEnabled()) {
            cb.enabled(cbc.getEnabled());
        }
        if (cbc.hasVolumeThreshold()) {
            cb.volumeThreshold(cbc.getVolumeThreshold());
        }
        if (cbc.hasErrorThresholdPercentage()) {
            cb.errorThresholdPercentage(cbc.getErrorThresholdPercentage());
        }
        if (cbc.hasSleepWindowMs()) {
            cb.sleepWindow(Duration.ofMillis(cbc.getSleepWindowMs()));
        }
        if (cbc.hasRollingWindowMs()) {
            cb.rollingWindow(Duration.ofMillis(cbc.getRollingWindowMs()));
        }
        if (cbc.hasCanaryTimeoutMs()) {
            // Even though the Java SDK was the pathfinding one for this RFC, oddly it doesn't have this param.
            throw new UnsupportedOperationException("Canary timeout not supported");
        }
    }
    // [end]

    private static void applyTransactionsConfig(ClusterConnectionCreateRequest request, Supplier<ClusterConnection> getCluster, ClusterEnvironment.Builder clusterEnvironment) {
        var tc = request.getClusterConfig().getTransactionsConfig();
        // [if:3.3.0]
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
        } catch (Throwable err) {
            throw new InternalPerformerFailure(new RuntimeException(err));
        }

        if (tc.hasMetadataCollection()) {
            builder.metadataCollection(TransactionKeyspace.create(tc.getMetadataCollection().getBucketName(),
                    tc.getMetadataCollection().getScopeName(),
                    tc.getMetadataCollection().getCollectionName()));
        }

        clusterEnvironment.transactionsConfig(builder);
        // [end]
    }

    private static void applyObservabilityConfig(ClusterEnvironment.Builder clusterEnvironment, ClusterConfig cc, ArrayList<Runnable> onClusterConnectionClose) {
        var oc = cc.getObservabilityConfig();

        // [if:3.2.0]
        if (oc.hasMetrics() || oc.hasTracing()) {
            SdkTracerProvider tracerProvider = null;
            SdkMeterProvider meterProvider = null;

            if (oc.hasTracing()) {
                var tc = oc.getTracing();
                var epsilon = 0.00001;
                var sampler = (tc.getSamplingPercentage() < epsilon)
                        ? Sampler.alwaysOff()
                        : (tc.getSamplingPercentage() > (1.0 - epsilon))
                        ? Sampler.alwaysOn()
                        : Sampler.traceIdRatioBased(tc.getSamplingPercentage());

                var exporter = OtlpGrpcSpanExporter.builder()
                        .setCompression("gzip")
                        .setEndpoint(tc.getEndpointHostname())
                        .build();

                var processor = tc.getBatching()
                        ? BatchSpanProcessor.builder(exporter)
                        .setScheduleDelay(Duration.ofMillis(tc.getExportEveryMillis()))
                        .build()
                        : SimpleSpanProcessor.create(exporter);

                ResourceBuilder resource = createOpenTelemetryResource(tc.getResourcesMap());

                tracerProvider = SdkTracerProvider.builder()
                        .setResource(Resource.getDefault().merge(resource.build()))
                        .addSpanProcessor(processor)
                        .setSampler(sampler)
                        .build();
            }

            if (oc.hasMetrics()) {
                var mc = oc.getMetrics();
                var exporter = OtlpGrpcMetricExporter.builder()
                        .setCompression("gzip")
                        .setEndpoint(mc.getEndpointHostname())
                        .build();

                ResourceBuilder resource = createOpenTelemetryResource(mc.getResourcesMap());

                meterProvider = SdkMeterProvider.builder()
                        .setResource(Resource.getDefault().merge(resource.build()))
                        .registerMetricReader(PeriodicMetricReader.builder(exporter)
                                .setInterval(Duration.ofMillis(mc.getExportEveryMillis()))
                                .build())
                        .build();
            }

            var openTelemetry = OpenTelemetrySdk.builder()
                    .setTracerProvider(tracerProvider)
                    .setMeterProvider(meterProvider)
                    .build();

            if (oc.hasMetrics()) {
                final SdkMeterProvider meterProviderForShutdown = meterProvider;
                onClusterConnectionClose.add(() -> {
                    logger.info("Shutting down meter provider");
                    meterProviderForShutdown.forceFlush();
                    meterProviderForShutdown.shutdown();
                });
                clusterEnvironment.meter(OpenTelemetryMeter.wrap(openTelemetry));
            }
            if (oc.hasTracing()) {
                // [end]
                // [if:3.5.0]
                final SdkTracerProvider tracerProviderForShutdown = tracerProvider;
                onClusterConnectionClose.add(() -> {
                    logger.info("Shutting down tracer provider");
                    tracerProviderForShutdown.forceFlush();
                    tracerProviderForShutdown.shutdown();
                });
                var tracer = OpenTelemetryRequestTracer.wrap(openTelemetry);
                clusterEnvironment.requestTracer(tracer);
                // [end]
                // [if:3.2.0]
            }
        }

        if (oc.getUseNoopTracer()) {
            clusterEnvironment.requestTracer(NoopRequestTracer.INSTANCE);
        }

        if (oc.hasThresholdLoggingTracer()) {
            var tlc = oc.getThresholdLoggingTracer();
            var builder = ThresholdLoggingTracerConfig.builder();
            if (tlc.hasEmitIntervalMillis()) {
                builder.emitInterval(Duration.ofMillis(tlc.getEmitIntervalMillis()));
            }
            if (tlc.hasKvThresholdMillis()) {
                builder.kvThreshold(Duration.ofMillis(tlc.getKvThresholdMillis()));
            }
            if (tlc.hasQueryThresholdMillis()) {
                builder.queryThreshold(Duration.ofMillis(tlc.getQueryThresholdMillis()));
            }
            if (tlc.hasViewsThresholdMillis()) {
                builder.viewThreshold(Duration.ofMillis(tlc.getViewsThresholdMillis()));
            }
            if (tlc.hasSearchThresholdMillis()) {
                builder.searchThreshold(Duration.ofMillis(tlc.getSearchThresholdMillis()));
            }
            if (tlc.hasAnalyticsThresholdMillis()) {
                builder.analyticsThreshold(Duration.ofMillis(tlc.getAnalyticsThresholdMillis()));
            }
            // [end]
            // [if:3.4.0]
            if (tlc.hasTransactionsThresholdMillis()) {
                builder.transactionsThreshold(Duration.ofMillis(tlc.getTransactionsThresholdMillis()));
            }
            // [end]
            // [if:3.2.0]
            if (tlc.hasSampleSize()) {
                builder.sampleSize(tlc.getSampleSize());
            }
            if (tlc.hasEnabled()) {
                builder.enabled(tlc.getEnabled());
            }
            clusterEnvironment.thresholdLoggingTracerConfig(builder);
        }

        if (oc.hasLoggingMeter()) {
            var lm = oc.getLoggingMeter();
            var builder = LoggingMeterConfig.builder();
            if (lm.hasEmitIntervalMillis()) {
                builder.emitInterval(Duration.ofMillis(lm.getEmitIntervalMillis()));
            }
            if (lm.hasEnabled()) {
                builder.enabled(lm.getEnabled());
            }
            clusterEnvironment.loggingMeterConfig(builder);
        }

        if (oc.hasOrphanResponse()) {
            var om = oc.getOrphanResponse();
            var builder = com.couchbase.client.core.env.OrphanReporterConfig.builder();
            if (om.hasEmitIntervalMillis()) {
                builder.emitInterval(Duration.ofMillis(om.getEmitIntervalMillis()));
            }
            if (om.hasSampleSize()) {
                builder.sampleSize(om.getSampleSize());
            }
            if (om.hasEnabled()) {
                builder.enabled(om.getEnabled());
            }
            clusterEnvironment.orphanReporterConfig(builder);
        }

        // [end]
    }

    private static ResourceBuilder createOpenTelemetryResource(Map<String, Attribute> resources) {
        var resource = Resource.builder();
        resources.forEach((k, v) -> {
            if (v.hasValueBoolean()) {
                resource.put(k, v.getValueBoolean());
            } else if (v.hasValueLong()) {
                resource.put(k, v.getValueLong());
            } else if (v.hasValueString()) {
                resource.put(k, v.getValueString());
            } else throw new UnsupportedOperationException();
        });
        return resource;
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

    // [if:3.3.0]
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
                queryOptions.profile(QueryProfile.valueOf(qo.getProfile().toUpperCase()));
            }

            if (qo.hasReadonly()) {
                queryOptions.readonly(qo.getReadonly());
            }

            if (qo.getParametersPositionalCount() > 0) {
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

            if (qo.hasClientContextId()) {
              queryOptions.clientContextId(qo.getClientContextId());
            }
        }
        return queryOptions;
    }

    public static @Nullable TransactionOptions makeTransactionOptions(ClusterConnection connection,
                                                                      TransactionCreateRequest req,
                                                                      ConcurrentHashMap<String, RequestSpan> spans) {
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

            if (to.hasParentSpanId()) {
                ptcb.parentSpan(spans.get(to.getParentSpanId()));
            }
        }
        return ptcb;
    }
    // [end]

  public static Duration convertDuration(com.google.protobuf.Duration duration) {
      var nanos = duration.getNanos() + TimeUnit.SECONDS.toNanos(duration.getSeconds());
      return Duration.ofNanos(nanos);
  }
}
