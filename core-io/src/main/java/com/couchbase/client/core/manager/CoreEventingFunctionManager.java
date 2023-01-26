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
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;

import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;

/**
 * Encapsulates common functionality around the eventing management APIs.
 */
@Stability.Internal
public class CoreEventingFunctionManager {

  private static final String V1 = "/api/v1";

  private final Core core;
  private final CoreHttpClient httpClient;

  public CoreEventingFunctionManager(final Core core) {
    this.core = core;
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

  public CompletableFuture<Void> upsertFunction(final String name, byte[] function, final CoreCommonOptions options) {
    checkIfProtostellar();
    return httpClient
      .post(path(pathForFunction(name)), options).json(function)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_UPSERT)
      .build()
      .exec(core)
      .thenApply(response -> null);
  }

  public CompletableFuture<Void> dropFunction(final String name, final CoreCommonOptions options) {
    checkIfProtostellar();
    return httpClient
      .delete(path(pathForFunction(name)), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_DROP)
      .build()
      .exec(core)
      .thenApply(response -> null);
  }

  public CompletableFuture<Void> deployFunction(final String name, final CoreCommonOptions options) {
    checkIfProtostellar();
    return httpClient
      .post(path(pathForDeploy(name)), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_DEPLOY)
      .build()
      .exec(core)
      .thenApply(response -> null);
  }

  public CompletableFuture<byte[]> getAllFunctions(final CoreCommonOptions options) {
    checkIfProtostellar();
    return httpClient
      .get(path(pathForFunctions()), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_GET_ALL)
      .build()
      .exec(core)
      .thenApply(CoreHttpResponse::content);
  }

  public CompletableFuture<byte[]> getFunction(final String name, final CoreCommonOptions options) {
    checkIfProtostellar();
    return httpClient
      .get(path(pathForFunction(name)), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_GET)
      .build()
      .exec(core)
      .thenApply(CoreHttpResponse::content);
  }

  public CompletableFuture<Void> pauseFunction(final String name, final CoreCommonOptions options) {
    checkIfProtostellar();
    return httpClient
      .post(path(pathForPause(name)), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_PAUSE)
      .build()
      .exec(core)
      .thenApply(response -> null);
  }

  public CompletableFuture<Void> resumeFunction(final String name, final CoreCommonOptions options) {
    checkIfProtostellar();
    return httpClient
      .post(path(pathForResume(name)), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_RESUME)
      .build()
      .exec(core)
      .thenApply(response -> null);
  }

  public CompletableFuture<Void> undeployFunction(final String name, final CoreCommonOptions options) {
    checkIfProtostellar();
    return httpClient
      .post(path(pathForUndeploy(name)), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_UNDEPLOY)
      .build()
      .exec(core)
      .thenApply(response -> null);
  }

  public CompletableFuture<byte[]> functionsStatus(final CoreCommonOptions options) {
    checkIfProtostellar();
    return httpClient
      .get(path(pathForStatus()), options)
      .trace(TracingIdentifiers.SPAN_REQUEST_ME_STATUS)
      .build()
      .exec(core)
      .thenApply(CoreHttpResponse::content);
  }

  private void checkIfProtostellar() {
    if (core.isProtostellar()) {
      throw CoreProtostellarUtil.unsupportedInProtostellar("eventing function management");
    }
  }
}
