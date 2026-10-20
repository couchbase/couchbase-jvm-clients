/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.eventing;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.ReactiveScope;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.manager.eventing.ReactiveScopeEventingFunctionManager;
import com.couchbase.client.java.manager.eventing.ScopeEventingFunctionManager;
import com.couchbase.client.java.manager.eventing.DeployFunctionOptions;
import com.couchbase.client.java.manager.eventing.DropFunctionOptions;
import com.couchbase.client.java.manager.eventing.EventingFunction;
import com.couchbase.client.java.manager.eventing.EventingFunctionBucketBinding;
import com.couchbase.client.java.manager.eventing.EventingFunctionConstantBinding;
import com.couchbase.client.java.manager.eventing.EventingFunctionDcpBoundary;
import com.couchbase.client.java.manager.eventing.EventingFunctionDeploymentStatus;
import com.couchbase.client.java.manager.eventing.EventingFunctionKeyspace;
import com.couchbase.client.java.manager.eventing.EventingFunctionLanguageCompatibility;
import com.couchbase.client.java.manager.eventing.EventingFunctionLogLevel;
import com.couchbase.client.java.manager.eventing.EventingFunctionManager;
import com.couchbase.client.java.manager.eventing.EventingFunctionProcessingStatus;
import com.couchbase.client.java.manager.eventing.EventingFunctionSettings;
import com.couchbase.client.java.manager.eventing.EventingFunctionState;
import com.couchbase.client.java.manager.eventing.EventingFunctionUrlAuth;
import com.couchbase.client.java.manager.eventing.EventingFunctionUrlAuthBasic;
import com.couchbase.client.java.manager.eventing.EventingFunctionUrlAuthBearer;
import com.couchbase.client.java.manager.eventing.EventingFunctionUrlAuthDigest;
import com.couchbase.client.java.manager.eventing.EventingFunctionUrlBinding;
import com.couchbase.client.java.manager.eventing.EventingStatus;
import com.couchbase.client.java.manager.eventing.FunctionsStatusOptions;
import com.couchbase.client.java.manager.eventing.GetAllFunctionsOptions;
import com.couchbase.client.java.manager.eventing.GetFunctionOptions;
import com.couchbase.client.java.manager.eventing.PauseFunctionOptions;
import com.couchbase.client.java.manager.eventing.ReactiveEventingFunctionManager;
import com.couchbase.client.java.manager.eventing.ResumeFunctionOptions;
import com.couchbase.client.java.manager.eventing.UndeployFunctionOptions;
import com.couchbase.client.java.manager.eventing.UpsertFunctionOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Command;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.couchbase.client.performer.core.util.TimeUtil.getTimeNow;
import static com.couchbase.utils.OptionsUtil.convertDuration;
import static com.couchbase.utils.UserSchedulerUtil.withSchedulerCheck;

public class EventingHelper {

  private EventingHelper() {

  }

  public static void handleEventingFunctionManager(Cluster cluster,
                                                   ConcurrentHashMap<String, RequestSpan> spans,
                                                   com.couchbase.client.protocol.sdk.Command command,
                                                   Result.Builder result) {
    var efm = command.getClusterCommand().getEventingFunctionManager();
    handle(Ops.blocking(cluster.eventingFunctions()), efm, spans, result).block();
  }

  public static Mono<Result> handleEventingFunctionManagerReactive(ReactiveCluster cluster,
                                                                   ConcurrentHashMap<String, RequestSpan> spans,
                                                                   com.couchbase.client.protocol.sdk.Command command,
                                                                   Result.Builder result) {
    var efm = command.getClusterCommand().getEventingFunctionManager();
    return handle(Ops.reactive(cluster.eventingFunctions()), efm, spans, result);
  }

  public static void handleScopeEventingFunctionManager(Scope scope,
                                                        ConcurrentHashMap<String, RequestSpan> spans,
                                                        com.couchbase.client.protocol.sdk.Command command,
                                                        Result.Builder result) {
    var efm = toClusterCommand(command.getScopeCommand().getEventingFunctionManager());
    handle(Ops.blocking(scope.eventingFunctions()), efm, spans, result).block();
  }

  public static Mono<Result> handleScopeEventingFunctionManagerReactive(ReactiveScope scope,
                                                                        ConcurrentHashMap<String, RequestSpan> spans,
                                                                        com.couchbase.client.protocol.sdk.Command command,
                                                                        Result.Builder result) {
    var efm = toClusterCommand(command.getScopeCommand().getEventingFunctionManager());
    return handle(Ops.reactive(scope.eventingFunctions()), efm, spans, result);
  }

  private static Command toClusterCommand(com.couchbase.client.protocol.sdk.scope.eventingfunctionmanager.Command in) {
    var out = Command.newBuilder();
    switch (in.getCommandCase()) {
      case GET_FUNCTION -> out.setGetFunction(in.getGetFunction());
      case GET_ALL_FUNCTIONS -> out.setGetAllFunctions(in.getGetAllFunctions());
      case DEPLOY_FUNCTION -> out.setDeployFunction(in.getDeployFunction());
      case DROP_FUNCTION -> out.setDropFunction(in.getDropFunction());
      case FUNCTIONS_STATUS -> out.setFunctionsStatus(in.getFunctionsStatus());
      case PAUSE_FUNCTION -> out.setPauseFunction(in.getPauseFunction());
      case RESUME_FUNCTION -> out.setResumeFunction(in.getResumeFunction());
      case UNDEPLOY_FUNCTION -> out.setUndeployFunction(in.getUndeployFunction());
      case UPSERT_FUNCTION -> out.setUpsertFunction(in.getUpsertFunction());
      default -> throw new UnsupportedOperationException("Unknown scope eventing function manager command " + in.getCommandCase());
    }
    return out.build();
  }

  private static Mono<Result> handle(Ops ops,
                                     Command efm,
                                     ConcurrentHashMap<String, RequestSpan> spans,
                                     Result.Builder result) {
    return Mono.defer(() -> {
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();

      Mono<com.couchbase.client.protocol.sdk.Result> response = switch (efm.getCommandCase()) {
        case UPSERT_FUNCTION -> {
          var req = efm.getUpsertFunction();
          var o = req.getOptions();
          var options = options(UpsertFunctionOptions.upsertFunctionOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          yield ops.upsertFunction().apply(toSdk(req.getFunction()), options).then(Mono.fromSupplier(EventingHelper::success));
        }
        case GET_FUNCTION -> {
          var req = efm.getGetFunction();
          var o = req.getOptions();
          var options = options(GetFunctionOptions.getFunctionOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          yield ops.getFunction().apply(req.getName(), options)
                  .map(f -> eventingResult(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Result.newBuilder()
                          .setEventingFunction(toFit(f))));
        }
        case GET_ALL_FUNCTIONS -> {
          var o = efm.getGetAllFunctions().getOptions();
          var options = options(GetAllFunctionsOptions.getAllFunctionsOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          yield ops.getAllFunctions().apply(options)
                  .map(fs -> eventingResult(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Result.newBuilder()
                          .setEventingFunctionList(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionList.newBuilder()
                                  .addAllFunctions(fs.stream().map(EventingHelper::toFit).toList()))));
        }
        case DEPLOY_FUNCTION -> {
          var req = efm.getDeployFunction();
          var o = req.getOptions();
          var options = options(DeployFunctionOptions.deployFunctionOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          yield ops.deployFunction().apply(req.getName(), options).then(Mono.fromSupplier(EventingHelper::success));
        }
        case UNDEPLOY_FUNCTION -> {
          var req = efm.getUndeployFunction();
          var o = req.getOptions();
          var options = options(UndeployFunctionOptions.undeployFunctionOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          yield ops.undeployFunction().apply(req.getName(), options).then(Mono.fromSupplier(EventingHelper::success));
        }
        case DROP_FUNCTION -> {
          var req = efm.getDropFunction();
          var o = req.getOptions();
          var options = options(DropFunctionOptions.dropFunctionOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          yield ops.dropFunction().apply(req.getName(), options).then(Mono.fromSupplier(EventingHelper::success));
        }
        case PAUSE_FUNCTION -> {
          var req = efm.getPauseFunction();
          var o = req.getOptions();
          var options = options(PauseFunctionOptions.pauseFunctionOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          yield ops.pauseFunction().apply(req.getName(), options).then(Mono.fromSupplier(EventingHelper::success));
        }
        case RESUME_FUNCTION -> {
          var req = efm.getResumeFunction();
          var o = req.getOptions();
          var options = options(ResumeFunctionOptions.resumeFunctionOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          yield ops.resumeFunction().apply(req.getName(), options).then(Mono.fromSupplier(EventingHelper::success));
        }
        case FUNCTIONS_STATUS -> {
          var o = efm.getFunctionsStatus().getOptions();
          var options = options(FunctionsStatusOptions.functionsStatusOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          yield ops.functionsStatus().apply(options)
                  .map(s -> eventingResult(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Result.newBuilder()
                          .setEventingStatus(toFit(s))));
        }
        default -> Mono.error(new UnsupportedOperationException("Unknown eventing function manager command " + efm.getCommandCase()));
      };

      return response.map(r -> {
        result.setElapsedNanos(System.nanoTime() - start);
        result.setSdk(r);
        return result.build();
      });
    });
  }

  private static <T extends CommonOptions<T>> T options(T options,
                                                        boolean hasTimeout,
                                                        com.google.protobuf.Duration timeout,
                                                        boolean hasParentSpanId,
                                                        String parentSpanId,
                                                        ConcurrentHashMap<String, RequestSpan> spans) {
    if (hasTimeout) options.timeout(convertDuration(timeout));
    if (hasParentSpanId) options.parentSpan(spans.get(parentSpanId));
    return options;
  }

  private static com.couchbase.client.protocol.sdk.Result success() {
    return com.couchbase.client.protocol.sdk.Result.newBuilder().setSuccess(true).build();
  }

  private static com.couchbase.client.protocol.sdk.Result eventingResult(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Result.Builder result) {
    return com.couchbase.client.protocol.sdk.Result.newBuilder().setEventingFunctionManagerResult(result).build();
  }

  /**
   * Maps between FIT and SDK enums that share constant names.
   */
  private static <T extends Enum<T>> T byName(Class<T> type, Enum<?> value) {
    return Enum.valueOf(type, value.name());
  }

  /**
   * As {@link #byName}, but returns null for SDK values that FIT does not know about.
   */
  @Nullable
  private static <T extends Enum<T>> T byNameOrNull(Class<T> type, @Nullable Enum<?> value) {
    if (value == null) return null;
    try {
      return byName(type, value);
    } catch (IllegalArgumentException err) {
      return null;
    }
  }

  // FIT -> SDK

  private static EventingFunction toSdk(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunction in) {
    // grahamp: eventing is an older style API, that uses the same classes for creation, updates and reads.  This leads to some fields being inappropriate on some paths.  We do not expect the driver to send these.
    if (in.hasVersion() || in.hasHandlerUuid() || in.hasFunctionInstanceId()) {
      throw new UnsupportedOperationException("The Java SDK does not support setting version, handler_uuid or function_instance_id");
    }

    var out = EventingFunction.builder(in.getName(), in.getCode(), toSdk(in.getSourceKeyspace()), toSdk(in.getMetadataKeyspace()))
            .bucketBindings(in.getBucketBindingsList().stream().map(EventingHelper::toSdk).toList())
            .urlBindings(in.getUrlBindingsList().stream().map(EventingHelper::toSdk).toList())
            .constantBindings(in.getConstantBindingsList().stream()
                    .map(b -> EventingFunctionConstantBinding.create(b.getAlias(), b.getLiteral()))
                    .toList());
    if (in.hasEnforceSchema()) out.enforceSchema(in.getEnforceSchema());
    if (in.hasSettings()) out.settings(toSdk(in.getSettings()));
    return out.build();
  }

  private static EventingFunctionKeyspace toSdk(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionKeyspace in) {
    return EventingFunctionKeyspace.create(in.getBucket(),
            in.hasScope() ? in.getScope() : null,
            in.hasCollection() ? in.getCollection() : null);
  }

  private static EventingFunctionBucketBinding toSdk(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionBucketBinding in) {
    return switch (in.getAccess()) {
      case READ_ONLY -> EventingFunctionBucketBinding.createReadOnly(in.getAlias(), toSdk(in.getName()));
      case READ_WRITE -> EventingFunctionBucketBinding.createReadWrite(in.getAlias(), toSdk(in.getName()));
      default -> throw new UnsupportedOperationException("Unknown bucket access " + in.getAccess());
    };
  }

  private static EventingFunctionUrlBinding toSdk(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlBinding in) {
    var out = EventingFunctionUrlBinding.builder(in.getHostname(), in.getAlias())
            .allowCookies(in.getAllowCookies())
            .validateSslCertificate(in.getValidateSslCertificate());
    if (in.hasAuth()) {
      var auth = in.getAuth();
      out.auth(switch (auth.getAuthCase()) {
        case BASIC_AUTH -> EventingFunctionUrlAuth.basicAuth(auth.getBasicAuth().getUsername(), auth.getBasicAuth().getPassword());
        case DIGEST_AUTH -> EventingFunctionUrlAuth.digestAuth(auth.getDigestAuth().getUsername(), auth.getDigestAuth().getPassword());
        case BEARER_AUTH -> EventingFunctionUrlAuth.bearerAuth(auth.getBearerAuth().getKey());
        case NO_AUTH, AUTH_NOT_SET -> EventingFunctionUrlAuth.noAuth();
      });
    }
    return out.build();
  }

  private static EventingFunctionSettings toSdk(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionSettings in) {
    // grahamp: eventing is an older style API, that uses the same classes for creation, updates and reads.  This leads to some fields being inappropriate on some paths.  We do not expect the driver to send these.
    if (in.hasDeploymentStatus() || in.hasProcessingStatus()) {
      throw new UnsupportedOperationException("The Java SDK does not support setting deployment_status or processing_status");
    }

    var out = EventingFunctionSettings.builder();
    if (in.hasAppLogDir()) out.appLogDir(in.getAppLogDir());
    if (in.hasAppLogMaxFiles()) out.appLogMaxFiles(in.getAppLogMaxFiles());
    if (in.hasAppLogMaxSize()) out.appLogMaxSize(in.getAppLogMaxSize());
    if (in.hasBucketCacheAge()) out.bucketCacheAge(in.getBucketCacheAge());
    if (in.hasBucketCacheSize()) out.bucketCacheSize(in.getBucketCacheSize());
    if (in.hasCheckpointInterval()) out.checkpointInterval(convertDuration(in.getCheckpointInterval()));
    if (in.hasCppWorkerThreadCount()) out.cppWorkerThreadCount(in.getCppWorkerThreadCount());
    if (in.hasCurlMaxAllowedRespSize()) out.curlMaxAllowedRespSize(in.getCurlMaxAllowedRespSize());
    if (in.hasDcpStreamBoundary()) out.dcpStreamBoundary(byName(EventingFunctionDcpBoundary.class, in.getDcpStreamBoundary()));
    if (in.hasDescription()) out.description(in.getDescription());
    if (in.hasEnableAppLogRotation()) out.enableAppLogRotation(in.getEnableAppLogRotation());
    if (in.hasExecutionTimeout()) out.executionTimeout(convertDuration(in.getExecutionTimeout()));
    if (in.getHandlerFootersCount() > 0) out.handlerFooters(in.getHandlerFootersList());
    if (in.getHandlerHeadersCount() > 0) out.handlerHeaders(in.getHandlerHeadersList());
    if (in.hasLanguageCompatibility()) out.languageCompatibility(byName(EventingFunctionLanguageCompatibility.class, in.getLanguageCompatibility()));
    if (in.hasLcbInstCapacity()) out.lcbInstCapacity(in.getLcbInstCapacity());
    if (in.hasLcbRetryCount()) out.lcbRetryCount(in.getLcbRetryCount());
    if (in.hasLcbTimeout()) out.lcbTimeout(convertDuration(in.getLcbTimeout()));
    if (in.hasLogLevel()) out.logLevel(byName(EventingFunctionLogLevel.class, in.getLogLevel()));
    if (in.hasNumTimerPartitions()) out.numTimerPartitions(in.getNumTimerPartitions());
    if (in.hasQueryConsistency()) out.queryConsistency(byName(QueryScanConsistency.class, in.getQueryConsistency()));
    if (in.hasQueryPrepareAll()) out.queryPrepareAll(in.getQueryPrepareAll());
    if (in.hasSockBatchSize()) out.sockBatchSize(in.getSockBatchSize());
    if (in.hasTickDuration()) out.tickDuration(convertDuration(in.getTickDuration()));
    if (in.hasTimerContextSize()) out.timerContextSize(in.getTimerContextSize());
    if (in.hasUserPrefix()) out.userPrefix(in.getUserPrefix());
    if (in.hasWorkerCount()) out.workerCount(in.getWorkerCount());
    return out.build();
  }

  // SDK -> FIT

  private static com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunction toFit(EventingFunction in) {
    var out = com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunction.newBuilder()
            .setName(in.name())
            .setCode(in.code())
            .setSourceKeyspace(toFit(in.sourceKeyspace()))
            .setMetadataKeyspace(toFit(in.metadataKeyspace()))
            .setEnforceSchema(in.enforceSchema());
    if (in.settings() != null) out.setSettings(toFit(in.settings()));
    if (in.version() != null) out.setVersion(in.version());
    if (in.handlerUuid() != 0) out.setHandlerUuid(in.handlerUuid());
    if (in.functionInstanceId() != null) out.setFunctionInstanceId(in.functionInstanceId());
    if (in.bucketBindings() != null) in.bucketBindings().forEach(b -> out.addBucketBindings(toFit(b)));
    if (in.urlBindings() != null) in.urlBindings().forEach(b -> out.addUrlBindings(toFit(b)));
    if (in.constantBindings() != null) in.constantBindings().forEach(b -> out.addConstantBindings(
            com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionConstantBinding.newBuilder()
                    .setAlias(b.alias())
                    .setLiteral(b.literal())));
    return out.build();
  }

  private static com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionKeyspace toFit(EventingFunctionKeyspace in) {
    var out = com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionKeyspace.newBuilder()
            .setBucket(in.bucket());
    if (in.scope() != null) out.setScope(in.scope());
    if (in.collection() != null) out.setCollection(in.collection());
    return out.build();
  }

  private static com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionBucketBinding toFit(EventingFunctionBucketBinding in) {
    return com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionBucketBinding.newBuilder()
            .setAlias(in.alias())
            .setName(toFit(in.keyspace()))
            .setAccess(byName(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionBucketAccess.class, in.access()))
            .build();
  }

  private static com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlBinding toFit(EventingFunctionUrlBinding in) {
    var auth = com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlAuth.newBuilder();
    if (in.auth() instanceof EventingFunctionUrlAuthBasic basic) {
      var b = com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlAuthBasic.newBuilder();
      if (basic.username() != null) b.setUsername(basic.username());
      if (basic.password() != null) b.setPassword(basic.password());
      auth.setBasicAuth(b);
    } else if (in.auth() instanceof EventingFunctionUrlAuthDigest digest) {
      var b = com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlAuthDigest.newBuilder();
      if (digest.username() != null) b.setUsername(digest.username());
      if (digest.password() != null) b.setPassword(digest.password());
      auth.setDigestAuth(b);
    } else if (in.auth() instanceof EventingFunctionUrlAuthBearer bearer) {
      var b = com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlAuthBearer.newBuilder();
      if (bearer.key() != null) b.setKey(bearer.key());
      auth.setBearerAuth(b);
    } else {
      auth.setNoAuth(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlNoAuth.getDefaultInstance());
    }

    return com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionUrlBinding.newBuilder()
            .setAlias(in.alias())
            .setHostname(in.hostname())
            .setAllowCookies(in.allowCookies())
            .setValidateSslCertificate(in.validateSslCertificate())
            .setAuth(auth)
            .build();
  }

  private static com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionSettings toFit(EventingFunctionSettings in) {
    var out = com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionSettings.newBuilder();

    // The SDK uses null for unset objects, and 0 for unset numbers.
    if (in.appLogDir() != null) out.setAppLogDir(in.appLogDir());
    if (in.appLogMaxFiles() != 0) out.setAppLogMaxFiles(in.appLogMaxFiles());
    if (in.appLogMaxSize() != 0) out.setAppLogMaxSize(in.appLogMaxSize());
    if (in.bucketCacheAge() != 0) out.setBucketCacheAge(in.bucketCacheAge());
    if (in.bucketCacheSize() != 0) out.setBucketCacheSize(in.bucketCacheSize());
    if (in.checkpointInterval() != null) out.setCheckpointInterval(toFit(in.checkpointInterval()));
    if (in.cppWorkerThreadCount() != 0) out.setCppWorkerThreadCount(in.cppWorkerThreadCount());
    if (in.curlMaxAllowedRespSize() != 0) out.setCurlMaxAllowedRespSize(in.curlMaxAllowedRespSize());
    var dcpStreamBoundary = byNameOrNull(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDcpBoundary.class, in.dcpStreamBoundary());
    if (dcpStreamBoundary != null) out.setDcpStreamBoundary(dcpStreamBoundary);
    if (in.deploymentStatus() != null) out.setDeploymentStatus(toFit(in.deploymentStatus()));
    if (in.description() != null) out.setDescription(in.description());
    out.setEnableAppLogRotation(in.enableAppLogRotation());
    if (in.executionTimeout() != null) out.setExecutionTimeout(toFit(in.executionTimeout()));
    if (in.handlerFooters() != null) out.addAllHandlerFooters(in.handlerFooters());
    if (in.handlerHeaders() != null) out.addAllHandlerHeaders(in.handlerHeaders());
    var languageCompatibility = byNameOrNull(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLanguageCompatibility.class, in.languageCompatibility());
    if (languageCompatibility != null) out.setLanguageCompatibility(languageCompatibility);
    if (in.lcbInstCapacity() != 0) out.setLcbInstCapacity(in.lcbInstCapacity());
    if (in.lcbRetryCount() != 0) out.setLcbRetryCount(in.lcbRetryCount());
    if (in.lcbTimeout() != null) out.setLcbTimeout(toFit(in.lcbTimeout()));
    var logLevel = byNameOrNull(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionLogLevel.class, in.logLevel());
    if (logLevel != null) out.setLogLevel(logLevel);
    if (in.numTimerPartitions() != 0) out.setNumTimerPartitions(in.numTimerPartitions());
    if (in.processingStatus() != null) out.setProcessingStatus(toFit(in.processingStatus()));
    var queryConsistency = byNameOrNull(com.couchbase.client.protocol.shared.ScanConsistency.class, in.queryConsistency());
    if (queryConsistency != null) out.setQueryConsistency(queryConsistency);
    out.setQueryPrepareAll(in.queryPrepareAll());
    if (in.sockBatchSize() != 0) out.setSockBatchSize(in.sockBatchSize());
    if (in.tickDuration() != null) out.setTickDuration(toFit(in.tickDuration()));
    if (in.timerContextSize() != 0) out.setTimerContextSize(in.timerContextSize());
    if (in.userPrefix() != null) out.setUserPrefix(in.userPrefix());
    if (in.workerCount() != 0) out.setWorkerCount(in.workerCount());
    return out.build();
  }

  private static com.google.protobuf.Duration toFit(java.time.Duration in) {
    return com.google.protobuf.Duration.newBuilder()
            .setSeconds(in.getSeconds())
            .setNanos(in.getNano())
            .build();
  }

  private static com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDeploymentStatus toFit(EventingFunctionDeploymentStatus in) {
    return in.isDeployed()
            ? com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDeploymentStatus.DEPLOYMENT_STATUS_DEPLOYED
            : com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionDeploymentStatus.DEPLOYMENT_STATUS_UNDEPLOYED;
  }

  private static com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionProcessingStatus toFit(EventingFunctionProcessingStatus in) {
    return in.isRunning()
            ? com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionProcessingStatus.PROCESSING_STATUS_RUNNING
            : com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionProcessingStatus.PROCESSING_STATUS_PAUSED;
  }

  private static com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingStatus toFit(EventingStatus in) {
    var out = com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingStatus.newBuilder()
            .setNumEventingNodes((int) in.numEventingNodes());
    in.functions().forEach(f -> out.addFunctions(toFit(f)));
    return out.build();
  }

  private static com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionState toFit(EventingFunctionState in) {
    var status = byNameOrNull(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionStatus.class, in.status());
    var out = com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionState.newBuilder()
            .setName(in.name())
            .setStatus(status != null ? status : com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.EventingFunctionStatus.UNKNOWN)
            .setNumBootstrappingNodes((int) in.numBootstrappingNodes())
            .setNumDeployedNodes((int) in.numDeployedNodes());
    if (in.deploymentStatus() != null) out.setDeploymentStatus(toFit(in.deploymentStatus()));
    if (in.processingStatus() != null) out.setProcessingStatus(toFit(in.processingStatus()));
    return out.build();
  }

  // Abstract over blocking vs reactive, scope vs global.
  private record Ops(
          BiFunction<EventingFunction, UpsertFunctionOptions, Mono<Void>> upsertFunction,
          BiFunction<String, GetFunctionOptions, Mono<EventingFunction>> getFunction,
          Function<GetAllFunctionsOptions, Mono<List<EventingFunction>>> getAllFunctions,
          BiFunction<String, DeployFunctionOptions, Mono<Void>> deployFunction,
          BiFunction<String, UndeployFunctionOptions, Mono<Void>> undeployFunction,
          BiFunction<String, DropFunctionOptions, Mono<Void>> dropFunction,
          BiFunction<String, PauseFunctionOptions, Mono<Void>> pauseFunction,
          BiFunction<String, ResumeFunctionOptions, Mono<Void>> resumeFunction,
          Function<FunctionsStatusOptions, Mono<EventingStatus>> functionsStatus) {

    static Ops blocking(EventingFunctionManager m) {
      return new Ops(
              (f, o) -> Mono.fromRunnable(() -> m.upsertFunction(f, o)),
              (n, o) -> Mono.fromCallable(() -> m.getFunction(n, o)),
              o -> Mono.fromCallable(() -> m.getAllFunctions(o)),
              (n, o) -> Mono.fromRunnable(() -> m.deployFunction(n, o)),
              (n, o) -> Mono.fromRunnable(() -> m.undeployFunction(n, o)),
              (n, o) -> Mono.fromRunnable(() -> m.dropFunction(n, o)),
              (n, o) -> Mono.fromRunnable(() -> m.pauseFunction(n, o)),
              (n, o) -> Mono.fromRunnable(() -> m.resumeFunction(n, o)),
              o -> Mono.fromCallable(() -> m.functionsStatus(o)));
    }

    static Ops reactive(ReactiveEventingFunctionManager m) {
      return new Ops(
              (f, o) -> withSchedulerCheck(m.upsertFunction(f, o)),
              (n, o) -> withSchedulerCheck(m.getFunction(n, o)),
              o -> withSchedulerCheck(m.getAllFunctions(o)).collectList(),
              (n, o) -> withSchedulerCheck(m.deployFunction(n, o)),
              (n, o) -> withSchedulerCheck(m.undeployFunction(n, o)),
              (n, o) -> withSchedulerCheck(m.dropFunction(n, o)),
              (n, o) -> withSchedulerCheck(m.pauseFunction(n, o)),
              (n, o) -> withSchedulerCheck(m.resumeFunction(n, o)),
              o -> withSchedulerCheck(m.functionsStatus(o)));
    }

    static Ops blocking(ScopeEventingFunctionManager m) {
      return new Ops(
              (f, o) -> Mono.fromRunnable(() -> m.upsertFunction(f, o)),
              (n, o) -> Mono.fromCallable(() -> m.getFunction(n, o)),
              o -> Mono.fromCallable(() -> m.getAllFunctions(o)),
              (n, o) -> Mono.fromRunnable(() -> m.deployFunction(n, o)),
              (n, o) -> Mono.fromRunnable(() -> m.undeployFunction(n, o)),
              (n, o) -> Mono.fromRunnable(() -> m.dropFunction(n, o)),
              (n, o) -> Mono.fromRunnable(() -> m.pauseFunction(n, o)),
              (n, o) -> Mono.fromRunnable(() -> m.resumeFunction(n, o)),
              o -> Mono.fromCallable(() -> m.functionsStatus(o)));
    }

    static Ops reactive(ReactiveScopeEventingFunctionManager m) {
      return new Ops(
              (f, o) -> withSchedulerCheck(m.upsertFunction(f, o)),
              (n, o) -> withSchedulerCheck(m.getFunction(n, o)),
              o -> withSchedulerCheck(m.getAllFunctions(o)).collectList(),
              (n, o) -> withSchedulerCheck(m.deployFunction(n, o)),
              (n, o) -> withSchedulerCheck(m.undeployFunction(n, o)),
              (n, o) -> withSchedulerCheck(m.dropFunction(n, o)),
              (n, o) -> withSchedulerCheck(m.pauseFunction(n, o)),
              (n, o) -> withSchedulerCheck(m.resumeFunction(n, o)),
              o -> withSchedulerCheck(m.functionsStatus(o)));
    }
  }
}
