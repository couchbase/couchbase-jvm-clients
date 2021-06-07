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
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.error.CompilationFailureException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.context.AnalyticsErrorContext;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.util.CbThrowables;
import com.couchbase.client.core.util.UrlQueryStringBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.endpoint.http.CoreHttpRequest.Builder.newForm;
import static com.couchbase.client.core.endpoint.http.CoreHttpRequest.Builder.newQueryString;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreAnalyticsLinkManager {
  public static final String S3_TYPE_NAME = "s3";
  public static final String AZURE_BLOB_TYPE_NAME = "azureblob";
  public static final String COUCHBASE_TYPE_NAME = "couchbase";

  private final Core core;
  private final CoreHttpClient httpClient;

  public CoreAnalyticsLinkManager(Core core) {
    this.core = requireNonNull(core);
    this.httpClient = core.httpClient(RequestTarget.analytics());
  }

  public CompletableFuture<Void> createLink(Map<String, String> link, CoreCommonOptions options) {
    return sendLink(HttpMethod.POST, link, options, TracingIdentifiers.SPAN_REQUEST_MA_CREATE_LINK);
  }

  public CompletableFuture<Void> replaceLink(Map<String, String> link, CoreCommonOptions options) {
    return sendLink(HttpMethod.PUT, link, options, TracingIdentifiers.SPAN_REQUEST_MA_REPLACE_LINK);
  }

  public CompletableFuture<Void> dropLink(String linkName, String dataverse, CoreCommonOptions options) {
    Map<String, String> link = new HashMap<>();
    link.put("dataverse", dataverse);
    link.put("name", linkName);
    return sendLink(HttpMethod.DELETE, link, options, TracingIdentifiers.SPAN_REQUEST_MA_DROP_LINK);
  }

  private CompletableFuture<Void> sendLink(HttpMethod method, Map<String, String> link, CoreCommonOptions options, String tracingId) {
    link = new HashMap<>(link); // ensure mutability, and don't modify caller's map
    CoreHttpPath path = getLinkPathAndAdjustMap(link);

    UrlQueryStringBuilder form = newForm();
    link.forEach(form::set);

    return httpClient.newRequest(method, path, options)
        .trace(tracingId)
        .form(form)
        .exec(core)
        .exceptionally(t -> {
          throw translateCompilationFailureToInvalidArgument(t);
        })
        .thenApply(result -> null);
  }

  private static RuntimeException translateCompilationFailureToInvalidArgument(Throwable t) {
    if (!(t.getCause() instanceof CompilationFailureException)) {
      CbThrowables.throwIfUnchecked(t);
      throw new CouchbaseException(t.getMessage(), t);
    }
    CompilationFailureException e = (CompilationFailureException) t.getCause();
    String message = ((AnalyticsErrorContext) e.context()).errors().get(0).message();
    throw new InvalidArgumentException(message, e.getCause(), e.context());
  }

  private static CoreHttpPath getLinkPathAndAdjustMap(Map<String, String> link) {
    String dataverse = requireNonNull(link.get("dataverse"), "Link is missing 'dataverse' property");
    String linkName = requireNonNull(link.get("name"), "Link is missing 'name' property");

    if (dataverse.contains("/")) {
      link.remove("dataverse");
      link.remove("name");
      return path("/analytics/link/{dataverse}/{linkName}", mapOf(
          "dataverse", dataverse,
          "linkName", linkName));
    }

    return path("/analytics/link");
  }

  /**
   * @param dataverseName (nullable)
   * @param linkType (nullable)
   * @param linkName (nullable) if present, dataverseName must also be present
   */
  public CompletableFuture<byte[]> getAllLinks(String dataverseName, String linkType, String linkName, CoreCommonOptions options) {
    if (linkName != null && dataverseName == null) {
      throw InvalidArgumentException.fromMessage("When link name is specified, must also specify dataverse");
    }

    UrlQueryStringBuilder queryString = newQueryString()
        .setIfNotNull("type", linkType);

    CoreHttpPath path = path("/analytics/link");
    if (dataverseName != null && dataverseName.contains("/")) {
      path = path.plus("/{dataverse}", mapOf("dataverse", dataverseName));
      if (linkName != null) {
        path = path.plus("/{linkName}", mapOf("linkName", linkName));
      }
    } else {
      queryString
          .setIfNotNull("dataverse", dataverseName)
          .setIfNotNull("name", linkName);
    }

    return httpClient.get(path, options)
        .queryString(queryString)
        .trace(TracingIdentifiers.SPAN_REQUEST_MA_GET_ALL_LINKS)
        .exec(core)
        .thenApply(CoreHttpResponse::content);
  }
}
