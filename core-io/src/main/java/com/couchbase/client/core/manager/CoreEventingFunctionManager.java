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

package com.couchbase.client.core.manager;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.RequestTarget;
import reactor.util.annotation.Nullable;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.formatPath;
import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;
import static java.util.Objects.requireNonNull;

/**
 * Encapsulates common functionality around the eventing management APIs.
 */
@Stability.Internal
public class CoreEventingFunctionManager {

  private static final String V1 = "/api/v1";

  private final Core core;
  @Nullable private final CoreBucketAndScope scope;
  private final CoreHttpClient httpClient;

  public CoreEventingFunctionManager(final Core core) {
    this(core, null);
  }

  public CoreEventingFunctionManager(
    final Core core,
    @Nullable final CoreBucketAndScope scope
  ) {
    this.core = requireNonNull(core);
    this.scope = scope;
    this.httpClient = core.httpClient(RequestTarget.eventing());
  }

  private static String pathForFunctions() {
    return V1 + "/functions";
  }

  private static String pathForFunction(final String name) {
    return pathForFunctions() + "/" + urlEncode(name);
  }

  private static String pathForDeploy(final String name) {
    return pathForFunction(name) + "/deploy";
  }

  private static String pathForUndeploy(final String name) {
    return pathForFunction(name) + "/undeploy";
  }

  private static String pathForResume(final String name) {
    return pathForFunction(name) + "/resume";
  }

  private static String pathForPause(final String name) {
    return pathForFunction(name) + "/pause";
  }

  private static String pathForStatus() {
    return V1 + "/status";
  }

  private CoreHttpPath scopedPath(String template) {
    if (scope != null) {
      template += formatPath("?bucket={}&scope={}", scope.bucketName(), scope.scopeName());
    }
    return CoreHttpPath.path(template);
  }

  private void setSpanAttributes(RequestSpan span) {
    span.lowCardinalityAttribute(TracingIdentifiers.ATTR_NAME, scope == null ? "*" : scope.bucketName());
    span.lowCardinalityAttribute(TracingIdentifiers.ATTR_SCOPE, scope == null ? "*" : scope.scopeName());
  }

  public CompletableFuture<Void> upsertFunction(final String name, byte[] function, final CoreCommonOptions options) {
    function = injectScope(function);

    return httpClient
      .post(scopedPath(pathForFunction(name)), options).json(function)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_UPSERT, this::setSpanAttributes)
      .build()
      .exec(core)
      .thenApply(response -> null);
  }

  private byte[] injectScope(byte[] function) {
    if (scope == null) {
      return function;
    }
    ObjectNode node = Mapper.decodeInto(function, ObjectNode.class);
    node.set(
      "function_scope",
      Mapper.createObjectNode()
        .put("bucket", scope.bucketName())
        .put("scope", scope.scopeName())
    );
    return Mapper.encodeAsBytes(node);
  }

  public CompletableFuture<Void> dropFunction(final String name, final CoreCommonOptions options) {
    return httpClient
      .delete(scopedPath(pathForFunction(name)), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_DROP, this::setSpanAttributes)
      .build()
      .exec(core)
      .thenApply(response -> null);
  }

  public CompletableFuture<Void> deployFunction(final String name, final CoreCommonOptions options) {
    return httpClient
      .post(scopedPath(pathForDeploy(name)), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_DEPLOY, this::setSpanAttributes)
      .build()
      .exec(core)
      .thenApply(response -> null);
  }

  public CompletableFuture<byte[]> getAllFunctions(final CoreCommonOptions options) {
    return httpClient
      .get(path(pathForFunctions()), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_GET_ALL, this::setSpanAttributes)
      .build()
      .exec(core)
      .thenApply(response -> filterGetAllFunctionsResponse(response.content()));
  }

  private byte[] filterGetAllFunctionsResponse(byte[] input) {
    ArrayNode node = Mapper.decodeInto(input, ArrayNode.class);
    applyScopeFilter(node);
    return Mapper.encodeAsBytes(node);
  }

  public CompletableFuture<byte[]> getFunction(final String name, final CoreCommonOptions options) {
    return httpClient
      .get(scopedPath(pathForFunction(name)), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_GET, this::setSpanAttributes)
      .build()
      .exec(core)
      .thenApply(CoreHttpResponse::content);
  }

  public CompletableFuture<Void> pauseFunction(final String name, final CoreCommonOptions options) {
    return httpClient
      .post(scopedPath(pathForPause(name)), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_PAUSE, this::setSpanAttributes)
      .build()
      .exec(core)
      .thenApply(response -> null);
  }

  public CompletableFuture<Void> resumeFunction(final String name, final CoreCommonOptions options) {
    return httpClient
      .post(scopedPath(pathForResume(name)), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_RESUME, this::setSpanAttributes)
      .build()
      .exec(core)
      .thenApply(response -> null);
  }

  public CompletableFuture<Void> undeployFunction(final String name, final CoreCommonOptions options) {
    return httpClient
      .post(scopedPath(pathForUndeploy(name)), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_UNDEPLOY, this::setSpanAttributes)
      .build()
      .exec(core)
      .thenApply(response -> null);
  }

  public CompletableFuture<byte[]> functionsStatus(final CoreCommonOptions options) {
    return httpClient
      .get(path(pathForStatus()), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_STATUS, this::setSpanAttributes)
      .build()
      .exec(core)
      .thenApply(response -> filterFunctionStatusResponse(response.content()));
  }

  private byte[] filterFunctionStatusResponse(byte[] input) {
    JsonNode node = Mapper.decodeIntoTree(input);
    applyScopeFilter((ArrayNode) node.get("apps"));
    return Mapper.encodeAsBytes(node);
  }

  /**
   * Removes from the array any elements whose "function_scope"
   * field does not match this manager's scope.
   */
  private void applyScopeFilter(@Nullable ArrayNode array) {
    if (array == null) {
      return;
    }
    for (Iterator<JsonNode> i = array.iterator(); i.hasNext(); ) {
      JsonNode element = i.next();
      JsonNode scopeNode = element.get("function_scope");
      if (!Objects.equals(scope, parseScope(scopeNode))) {
        i.remove();
      }
    }
  }

  private static final CoreBucketAndScope ADMIN_SCOPE = new CoreBucketAndScope("*", "*");

  private static @Nullable CoreBucketAndScope parseScope(@Nullable JsonNode node) {
    if (node == null) {
      return null;
    }
    CoreBucketAndScope result = new CoreBucketAndScope(
      node.path("bucket").asText(),
      node.path("scope").asText()
    );
    return result.equals(ADMIN_SCOPE) ? null : result;
  }
}
