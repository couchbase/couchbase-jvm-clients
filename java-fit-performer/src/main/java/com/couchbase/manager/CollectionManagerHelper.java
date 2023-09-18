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

package com.couchbase.manager;

// [skip:<3.4.11]

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.CollectionSpec;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.GetAllScopesOptions;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.GetAllScopesResult;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.ScopeSpec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


public class CollectionManagerHelper {

  private CollectionManagerHelper() {

  }

  public static void handleCollectionManager(Cluster cluster,
                                             ConcurrentHashMap<String, RequestSpan> spans,
                                             com.couchbase.client.protocol.sdk.Command command,
                                             Result.Builder result) {

    var cm = command.getBucketCommand().getCollectionManager();
    if (cm.hasGetAllScopes()) {
      var request = cm.getGetAllScopes();
      var bucketName = command.getBucketCommand().getBucketName();
      List<com.couchbase.client.java.manager.collection.ScopeSpec> response;
      if (!request.hasOptions()) {
        response = cluster.bucket(bucketName).collections().getAllScopes();
      } else {
        var options = createGetAllScopesOptions(request.getOptions(), spans);
        response = cluster.bucket(bucketName).collections().getAllScopes(options);
      }
      populateResult(result, response);
    }
  }

  public static Mono<Result> handleCollectionManagerReactive(ReactiveCluster cluster,
                                                             ConcurrentHashMap<String, RequestSpan> spans,
                                                             com.couchbase.client.protocol.sdk.Command command,
                                                             Result.Builder result) {


    var cm = command.getBucketCommand().getCollectionManager();
    if (cm.hasGetAllScopes()) {
      Flux<com.couchbase.client.java.manager.collection.ScopeSpec> response;
      var request = cm.getGetAllScopes();
      var bucketName = command.getBucketCommand().getBucketName();
      if (!request.hasOptions()) {
        response = cluster.bucket(bucketName).collections().getAllScopes();
      } else {
        var options = createGetAllScopesOptions(request.getOptions(), spans);
        response = cluster.bucket(bucketName).collections().getAllScopes(options);
      }
      Flux<Result> f = response.map(r -> {
        populateResult(result, r);
        return result.build();
      });
      return f.last();
    } else {
      return Mono.error(new UnsupportedOperationException(new IllegalArgumentException("Unknown operation")));
    }

  }

  public static void populateResult(Result.Builder result, List<com.couchbase.client.java.manager.collection.ScopeSpec> response) {
    var builder = GetAllScopesResult.newBuilder();

    var allResult = response.stream()
            .map(scopeSpec -> ScopeSpec.newBuilder()
                    .setName(scopeSpec.name())
                    .addAllCollections(scopeSpec.collections().stream()
                            .map(c -> CollectionSpec.newBuilder()
                                    .setName(c.name())
                                    .setExpirySecs(c.maxExpiry() != null ? (int) c.maxExpiry().toSeconds() : 0)
                                    .setScopeName(c.scopeName())
                                    .setHistory(c.history())
                                    .build()).collect(Collectors.toList()))
                    .build()).collect(Collectors.toList());

    builder.addAllResult(allResult);

    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setCollectionManagerResult(com.couchbase.client.protocol.sdk.bucket.collectionmanager.Result.newBuilder()
                    .setGetAllScopesResult(builder)));

  }

  public static void populateResult(Result.Builder result, com.couchbase.client.java.manager.collection.ScopeSpec scopeSpec) {
    var builder = GetAllScopesResult.newBuilder();

    var list = result.getSdkBuilder() != null ?
            result.getSdkBuilder().getCollectionManagerResult().getGetAllScopesResult().getResultList() : null;
    var allResult = ScopeSpec.newBuilder()
            .setName(scopeSpec.name())
            .addAllCollections(scopeSpec.collections().stream()
                    .map(c -> CollectionSpec.newBuilder()
                            .setName(c.name())
                            .setExpirySecs(c.maxExpiry() != null ? (int) c.maxExpiry().toSeconds() : 0)
                            .setScopeName(c.scopeName())
                            .setHistory(c.history())
                            .build()).collect(Collectors.toList()))
            .build();

    if (list != null) {
      builder.addAllResult(list);
    }
    builder.addResult(allResult);

    result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
            .setCollectionManagerResult(com.couchbase.client.protocol.sdk.bucket.collectionmanager.Result.newBuilder()
                    .setGetAllScopesResult(builder)));

  }

  private static com.couchbase.client.java.manager.collection.GetAllScopesOptions createGetAllScopesOptions(GetAllScopesOptions getAllScopesOptions, ConcurrentHashMap<String, RequestSpan> spans) {
    var options = com.couchbase.client.java.manager.collection.GetAllScopesOptions.getAllScopesOptions();

    if (getAllScopesOptions.hasTimeoutMsecs()) options.timeout(java.time.Duration.ofMillis(getAllScopesOptions.getTimeoutMsecs()));
    if (getAllScopesOptions.hasParentSpanId()) options.parentSpan(spans.get(getAllScopesOptions.getParentSpanId()));

    return options;
  }

}
