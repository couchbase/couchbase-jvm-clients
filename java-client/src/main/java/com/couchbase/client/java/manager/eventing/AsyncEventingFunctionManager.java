/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.java.manager.eventing;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.manager.CoreEventingFunctionManager;
import com.couchbase.client.java.query.QueryScanConsistency;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.java.manager.eventing.GetAllFunctionsOptions.getAllFunctionsOptions;

@Stability.Uncommitted
public class AsyncEventingFunctionManager {

  private final CoreEventingFunctionManager coreManager;

  public AsyncEventingFunctionManager(final Core core) {
    this.coreManager = new CoreEventingFunctionManager(core);
  }

  public CompletableFuture<Void> upsertFunction(final EventingFunction function) {
    return upsertFunction(function, UpsertFunctionOptions.upsertFunctionOptions());
  }

  public CompletableFuture<Void> upsertFunction(final EventingFunction function, final UpsertFunctionOptions options) {
    return coreManager.upsertFunction(function.name(), encodeFunction(function), options.build());
  }

  public CompletableFuture<EventingFunction> getFunction(final String name) {
    return getFunction(name, GetFunctionOptions.getFunctionOptions());
  }

  public CompletableFuture<EventingFunction> getFunction(final String name, final GetFunctionOptions options) {
    return coreManager
      .getFunction(name, options.build())
      .thenApply(AsyncEventingFunctionManager::decodeFunction);
  }

  public CompletableFuture<Void> dropFunction(final String name) {
    return dropFunction(name, DropFunctionOptions.dropFunctionOptions());
  }

  public CompletableFuture<Void> dropFunction(final String name, final DropFunctionOptions options) {
    return coreManager.dropFunction(name, options.build());
  }

  public CompletableFuture<Void> deployFunction(final String name) {
    return deployFunction(name, DeployFunctionOptions.deployFunctionOptions());
  }

  public CompletableFuture<Void> deployFunction(final String name, final DeployFunctionOptions options) {
    return coreManager.deployFunction(name, options.build());
  }

  public CompletableFuture<List<EventingFunction>> getAllFunctions() {
    return getAllFunctions(getAllFunctionsOptions());
  }
  public CompletableFuture<List<EventingFunction>> getAllFunctions(final GetAllFunctionsOptions options) {
    return coreManager
      .getAllFunctions(options.build())
      .thenApply(AsyncEventingFunctionManager::decodeFunctions);
  }

  public CompletableFuture<Void> pauseFunction(final String name) {
    return pauseFunction(name, PauseFunctionOptions.pauseFunctionOptions());
  }

  public CompletableFuture<Void> pauseFunction(final String name, final PauseFunctionOptions options) {
    return coreManager.pauseFunction(name, options.build());
  }

  public CompletableFuture<Void> resumeFunction(final String name) {
    return resumeFunction(name, ResumeFunctionOptions.resumeFunctionOptions());
  }

  public CompletableFuture<Void> resumeFunction(final String name, final ResumeFunctionOptions options) {
    return coreManager.resumeFunction(name, options.build());
  }

  public CompletableFuture<Void> undeployFunction(final String name) {
    return undeployFunction(name, UndeployFunctionOptions.undeployFunctionOptions());
  }

  public CompletableFuture<Void> undeployFunction(final String name, final UndeployFunctionOptions options) {
    return coreManager.undeployFunction(name, options.build());
  }

  public CompletableFuture<EventingStatus> functionsStatus() {
    return functionsStatus(FunctionsStatusOptions.functionsStatusOptions());
  }

  public CompletableFuture<EventingStatus> functionsStatus(final FunctionsStatusOptions options) {
    return coreManager
      .functionsStatus(options.build())
      .thenApply(bytes -> Mapper.decodeInto(bytes, EventingStatus.class));
  }

  private static byte[] encodeFunction(final EventingFunction function) {
    Map<String, Object> func = new HashMap<>();

    func.put("appname", function.name());
    func.put("appcode", function.code());
    if (function.version() != null) {
      func.put("version", function.version());
    }
    if (function.enforceSchema()) {
      func.put("enforce_schema", function.enforceSchema());
    }
    if (function.handlerUuid() != 0) {
      func.put("handleruuid", function.handlerUuid());
    }
    if (function.functionInstanceId() != null) {
      func.put("function_instance_id", function.functionInstanceId());
    }

    Map<String, Object> depcfg = new HashMap<>();

    depcfg.put("source_bucket", function.sourceKeyspace().bucket());
    depcfg.put("source_scope", function.sourceKeyspace().scope());
    depcfg.put("source_collection", function.sourceKeyspace().collection());
    depcfg.put("metadata_bucket", function.metadataKeyspace().bucket());
    depcfg.put("metadata_scope", function.metadataKeyspace().scope());
    depcfg.put("metadata_collection", function.metadataKeyspace().collection());

    if (function.constantBindings() != null && !function.constantBindings().isEmpty()) {
      List<Map<String, String>> constants = function
        .constantBindings()
        .stream()
        .map(c -> mapOf("alias", c.alias(), "literal", c.literal()))
        .collect(Collectors.toList());
      depcfg.put("constants", constants);
    }

    if (function.urlBindings() != null && !function.urlBindings().isEmpty()) {
      List<Map<String, Object>> urls = function.urlBindings().stream().map(c -> {
        Map<String, Object> map = new HashMap<>();
        map.put("alias", c.alias());
        map.put("hostname", c.hostname());
        map.put("allow_cookies", c.allowCookies());
        map.put("validate_ssl_certificates", c.validateSslCertificates());
        if (c.auth() instanceof EventingFunctionUrlNoAuth) {
          map.put("auth_type", "no-auth");
        } else if (c.auth() instanceof EventingFunctionUrlAuthBasic) {
          map.put("auth_type", "basic");
          map.put("username", ((EventingFunctionUrlAuthBasic) c.auth()).username());
          map.put("password", ((EventingFunctionUrlAuthBasic) c.auth()).password());
        } else if (c.auth() instanceof EventingFunctionUrlAuthDigest) {
          map.put("auth_type", "digest");

        } else if (c.auth() instanceof EventingFunctionUrlAuthBearer) {
          map.put("auth_type", "bearer");
        }
        return map;
      }).collect(Collectors.toList());
      depcfg.put("curl", urls);
    }

    if (function.bucketBindings() != null && !function.bucketBindings().isEmpty()) {
      List<Map<String, Object>> buckets = function.bucketBindings().stream().map(c -> {
        Map<String, Object> map = new HashMap<>();
        map.put("alias", c.alias());
        map.put("bucket_name", c.name().bucket());
        map.put("scope_name", c.name().scope());
        map.put("collection_name", c.name().collection());
        if (c.access() != null) {
          map.put("access", c.access() == EventingFunctionBucketAccess.READ_ONLY ? "r" : "rw");
        }
        return map;
      }).collect(Collectors.toList());
      depcfg.put("buckets", buckets);
    }

    Map<String, Object> settings = new HashMap<>();
    EventingFunctionSettings efs = function.settings();

    if (efs.processingStatus() != null) {
      settings.put("processing_status", efs.processingStatus().isRunning());
    } else {
      settings.put("processing_status", false);
    }
    if (efs.deploymentStatus() != null) {
      settings.put("deployment_status", efs.deploymentStatus().isDeployed());
    } else {
      settings.put("deployment_status", false);
    }
    if (efs.cppWorkerThreadCount() > 0) {
      settings.put("cpp_worker_thread_count", efs.cppWorkerThreadCount());
    }
    if (efs.dcpStreamBoundary() != null) {
      settings.put("dcp_stream_boundary", efs.dcpStreamBoundary());
    }
    if (efs.description() != null) {
      settings.put("description", efs.description());
    }
    if (efs.logLevel() != null) {
      settings.put("log_level", efs.logLevel());
    }
    if (efs.languageCompatibility() != null) {
      settings.put("language_compatibility", efs.languageCompatibility());
    }
    if (efs.executionTimeout() != null) {
      settings.put("execution_timeout", efs.executionTimeout().getSeconds());
    }
    if (efs.lcbTimeout() != null) {
      settings.put("lcb_timeout", efs.lcbTimeout().getSeconds());
    }
    if (efs.lcbInstCapacity() > 0) {
      settings.put("lcb_inst_capacity", efs.lcbInstCapacity());
    }
    if (efs.lcbRetryCount() > 0) {
      settings.put("lcb_retry_count", efs.lcbRetryCount());
    }
    if (efs.numTimerPartitions() > 0) {
      settings.put("num_timer_partitions", efs.numTimerPartitions());
    }
    if (efs.sockBatchSize() > 0) {
      settings.put("sock_batch_size", efs.sockBatchSize());
    }
    if (efs.tickDuration() != null ) {
      settings.put("tick_duration", efs.tickDuration().toMillis());
    }
    if (efs.timerContextSize() > 0) {
      settings.put("timer_context_size", efs.timerContextSize());
    }
    if (efs.bucketCacheSize() > 0) {
      settings.put("bucket_cache_size", efs.bucketCacheSize());
    }
    if (efs.bucketCacheAge() > 0) {
      settings.put("bucket_cache_age", efs.bucketCacheAge());
    }
    if (efs.curlMaxAllowedRespSize() > 0) {
      settings.put("curl_max_allowed_resp_size", efs.curlMaxAllowedRespSize());
    }
    if (efs.workerCount() > 0) {
      settings.put("worker_count", efs.workerCount());
    }
    if (efs.appLogMaxSize() > 0) {
      settings.put("app_log_max_size", efs.appLogMaxSize());
    }
    if (efs.appLogMaxFiles() > 0) {
      settings.put("app_log_max_files", efs.appLogMaxFiles());
    }
    if (efs.checkpointInterval() != null) {
      settings.put("checkpoint_interval", efs.checkpointInterval().getSeconds());
    }
    if (efs.handlerHeaders() != null && !efs.handlerHeaders().isEmpty()) {
      settings.put("handler_headers", efs.handlerHeaders());
    }
    if (efs.handlerFooters() != null && !efs.handlerFooters().isEmpty()) {
      settings.put("handler_footers", efs.handlerFooters());
    }
    if (efs.queryPrepareAll()) {
      settings.put("n1ql_prepare_all", efs.queryPrepareAll());
    }
    if (efs.enableAppLogRotation()) {
      settings.put("enable_applog_rotation", efs.enableAppLogRotation());
    }
    if (efs.userPrefix() != null) {
      settings.put("user_prefix", efs.userPrefix());
    }
    if (efs.appLogDir() != null) {
      settings.put("app_log_dir", efs.appLogDir());
    }
    if (efs.queryConsistency() != null) {
      String encoded = efs.queryConsistency() == QueryScanConsistency.REQUEST_PLUS ? "request" : "none";
      settings.put("n1ql_consistency", encoded);
    }

    func.put("depcfg", depcfg);
    func.put("settings", settings);

    return Mapper.encodeAsBytes(func);
  }

  private static EventingFunction decodeFunction(final byte[] encoded) {
    JsonNode func = Mapper.decodeIntoTree(encoded);
    JsonNode depcfg = func.get("depcfg");
    JsonNode settings = func.get("settings");

    String version = func.has("version") ? func.get("version").asText() : null;
    String functionInstanceId = func.has("function_instance_id") ? func.get("function_instance_id").asText() : null;
    int handlerUuid = func.has("handleruuid") ? func.get("handleruuid").asInt() : 0;

    EventingFunction.Builder toReturn = EventingFunction
      .builder(
        func.get("appname").asText(),
        func.get("appcode").asText(),
        EventingFunctionKeyspace.create(depcfg.get("source_bucket").asText(), depcfg.get("source_scope").asText(), depcfg.get("source_collection").asText()),
        EventingFunctionKeyspace.create(depcfg.get("metadata_bucket").asText(), depcfg.get("metadata_scope").asText(), depcfg.get("metadata_collection").asText())
      )
      .handlerUuid(handlerUuid)
      .functionInstanceId(functionInstanceId)
      .version(version);

    EventingFunctionSettings.Builder settingsBuilder = EventingFunctionSettings.builder();

    if (settings.has("deployment_status")) {
      settingsBuilder.deploymentStatus(settings.get("deployment_status").asBoolean()
        ? EventingFunctionDeploymentStatus.DEPLOYED
        : EventingFunctionDeploymentStatus.UNDEPLOYED);
    }
    if (settings.has("processing_status")) {
      settingsBuilder.processingStatus(settings.get("processing_status").asBoolean()
        ? EventingFunctionProcessingStatus.RUNNING
        : EventingFunctionProcessingStatus.PAUSED);
    }

    if (func.has("enforce_schema")) {
      toReturn.enforceSchema(func.get("enforce_schema").asBoolean());
    }

    if (settings.has("cpp_worker_thread_count")) {
      settingsBuilder.cppWorkerThreadCount(settings.get("cpp_worker_thread_count").asLong());
    }
    if (settings.has("dcp_stream_boundary")) {
      settingsBuilder.dcpStreamBoundary(settings.get("dcp_stream_boundary").asText());
    }
    if (settings.has("description")) {
      settingsBuilder.description(settings.get("description").asText());
    }
    if (settings.has("log_level")) {
      settingsBuilder.logLevel(settings.get("log_level").asText());
    }
    if (settings.has("language_compatibility")) {
      settingsBuilder.languageCompatibility(settings.get("language_compatibility").asText());
    }
    if (settings.has("lcb_inst_capacity")) {
      settingsBuilder.lcbInstCapacity(settings.get("lcb_inst_capacity").asLong());
    }
    if (settings.has("lcb_retry_count")) {
      settingsBuilder.lcbRetryCount(settings.get("lcb_retry_count").asLong());
    }
    if (settings.has("num_timer_partitions")) {
      settingsBuilder.numTimerPartitions(settings.get("num_timer_partitions").asLong());
    }
    if (settings.has("sock_batch_size")) {
      settingsBuilder.sockBatchSize(settings.get("sock_batch_size").asLong());
    }
    if (settings.has("tick_duration")) {
      settingsBuilder.tickDuration(Duration.ofMillis(settings.get("tick_duration").asLong()));
    }
    if (settings.has("timer_context_size")) {
      settingsBuilder.timerContextSize(settings.get("timer_context_size").asLong());
    }
    if (settings.has("bucket_cache_size")) {
      settingsBuilder.bucketCacheSize(settings.get("bucket_cache_size").asLong());
    }
    if (settings.has("bucket_cache_age")) {
      settingsBuilder.bucketCacheAge(settings.get("bucket_cache_age").asLong());
    }
    if (settings.has("curl_max_allowed_resp_size")) {
      settingsBuilder.curlMaxAllowedRespSize(settings.get("curl_max_allowed_resp_size").asLong());
    }
    if (settings.has("worker_count")) {
      settingsBuilder.workerCount(settings.get("worker_count").asLong());
    }
    if (settings.has("app_log_max_size")) {
      settingsBuilder.appLogMaxSize(settings.get("app_log_max_size").asLong());
    }
    if (settings.has("app_log_max_files")) {
      settingsBuilder.appLogMaxFiles(settings.get("app_log_max_files").asLong());
    }
    if (settings.has("checkpoint_interval")) {
      settingsBuilder.checkpointInterval(Duration.ofSeconds(settings.get("checkpoint_interval").asLong()));
    }
    if (settings.has("execution_timeout")) {
      settingsBuilder.executionTimeout(Duration.ofSeconds(settings.get("execution_timeout").asLong()));
    }
    if (settings.has("lcb_timeout")) {
      settingsBuilder.lcbTimeout(Duration.ofSeconds(settings.get("lcb_timeout").asLong()));
    }
    if (settings.has("user_prefix")) {
      settingsBuilder.userPrefix(settings.get("user_prefix").asText());
    }
    if (settings.has("app_log_dir")) {
      settingsBuilder.appLogDir(settings.get("app_log_dir").asText());
    }
    if (settings.has("n1ql_prepare_all")) {
      settingsBuilder.queryPrepareAll(settings.get("n1ql_prepare_all").asBoolean());
    }
    if (settings.has("enable_applog_rotation")) {
      settingsBuilder.enableAppLogRotation(settings.get("enable_applog_rotation").asBoolean());
    }
    if (settings.has("n1ql_consistency")) {
      if ("request".equals(settings.get("n1ql_consistency").asText())) {
        settingsBuilder.queryConsistency(QueryScanConsistency.REQUEST_PLUS);
      } else {
        settingsBuilder.queryConsistency(QueryScanConsistency.NOT_BOUNDED);
      }
    }
    if (settings.has("handler_headers")) {
      List<String> headers = new ArrayList<>();
      for (JsonNode entry : settings.get("handler_headers")) {
        headers.add(entry.asText());
      }
      settingsBuilder.handlerHeaders(headers);
    }
    if (settings.has("handler_footers")) {
      List<String> footers = new ArrayList<>();
      for (JsonNode entry : settings.get("handler_footers")) {
        footers.add(entry.asText());
      }
      settingsBuilder.handlerFooters(footers);
    }

    if (depcfg.has("buckets")) {
      List<EventingFunctionBucketBinding> bucketBindings = new ArrayList<>();
      for (JsonNode buckets : depcfg.get("buckets")) {
        String alias = buckets.get("alias").asText();
        EventingFunctionKeyspace keyspace = EventingFunctionKeyspace.create(
          buckets.get("bucket_name").asText(),
          buckets.get("scope_name").asText(),
          buckets.get("collection_name").asText()
        );
        if ("rw".equals(buckets.get("access").asText())) {
          bucketBindings.add(EventingFunctionBucketBinding.createReadWrite(alias, keyspace));
        } else {
          bucketBindings.add(EventingFunctionBucketBinding.createReadOnly(alias, keyspace));
        }
      }
      toReturn.bucketBindings(bucketBindings);
    }
    if (depcfg.has("constants")) {
      List<EventingFunctionConstantBinding> constantBindings = new ArrayList<>();
      for (JsonNode constants : depcfg.get("constants")) {
        constantBindings.add(EventingFunctionConstantBinding.create(
          constants.get("value").asText(),
          constants.get("literal").asText()
        ));
      }
      toReturn.constantBindings(constantBindings);
    }
    if (depcfg.has("curl")) {
      List<EventingFunctionUrlBinding> urlBindings = new ArrayList<>();
      for (JsonNode url : depcfg.get("curl")) {
        EventingFunctionUrlBinding.Builder binding = EventingFunctionUrlBinding.builder(
          url.get("hostname").asText(),
          url.get("value").asText()
        );
        if (url.has("allow_cookies")) {
          binding.allowCookies(url.get("allow_cookies").asBoolean());
        }
        if (url.has("validate_ssl_certificate")) {
          binding.validateSslCertificates(url.get("validate_ssl_certificate").asBoolean());
        }
        if (url.has("auth_type")) {
          switch(url.get("auth_type").asText()) {
            case "no-auth":
              binding.auth(new EventingFunctionUrlNoAuth());
              break;
            case "basic":
              binding.auth(new EventingFunctionUrlAuthBasic(
                url.get("username").asText(),
                null
              ));
              break;
            case "digest":
              binding.auth(new EventingFunctionUrlAuthDigest(
                url.get("username").asText(),
                null
              ));
              break;
            case "bearer":
              binding.auth(new EventingFunctionUrlAuthBearer(url.get("bearer_key").asText()));
              break;
          }
        }
        urlBindings.add(binding.build());
      }
      toReturn.urlBindings(urlBindings);
    }

    return toReturn.settings(settingsBuilder.build()).build();
  }

  private static List<EventingFunction> decodeFunctions(final byte[] encoded) {
    JsonNode encodedFunctions = Mapper.decodeIntoTree(encoded);

    List<EventingFunction> functions = new ArrayList<>();
    for (JsonNode encodedFunction : encodedFunctions) {
      functions.add(decodeFunction(Mapper.encodeAsBytes(encodedFunction)));
    }

    return functions;
  }


}
