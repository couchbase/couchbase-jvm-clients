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

// [skip:<3.4.12]

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.CollectionSpec;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.CreateCollectionOptions;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.CreateCollectionSettings;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.CreateScopeOptions;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.DropCollectionOptions;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.DropScopeOptions;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.GetAllScopesOptions;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.GetAllScopesResult;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.ScopeSpec;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.UpdateCollectionOptions;
import com.couchbase.client.protocol.sdk.bucket.collectionmanager.UpdateCollectionSettings;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.couchbase.JavaSdkCommandExecutor.setSuccess;


public class CollectionManagerHelper {

  private CollectionManagerHelper() {

  }

  public static void handleCollectionManager(Cluster cluster,
                                             ConcurrentHashMap<String, RequestSpan> spans,
                                             com.couchbase.client.protocol.sdk.Command command,
                                             Result.Builder result) {

    var cm = command.getBucketCommand().getCollectionManager();
    var bucketName = command.getBucketCommand().getBucketName();
    var collections = cluster.bucket(bucketName).collections();
    if (cm.hasGetAllScopes()) {
      var request = cm.getGetAllScopes();
      List<com.couchbase.client.java.manager.collection.ScopeSpec> response;
      if (!request.hasOptions()) {
        response = collections.getAllScopes();
      } else {
        var options = createGetAllScopesOptions(request.getOptions(), spans);
        response = collections.getAllScopes(options);
      }
      populateResult(result, response);
    } else if (cm.hasCreateScope()) {
      var request = cm.getCreateScope();
      if (request.hasOptions()) {
        var options = createScopeOptions(request.getOptions(), spans);
        collections.createScope(request.getName(), options);
      } else {
        collections.createScope(request.getName());
      }
      setSuccess(result);
    } else if (cm.hasDropScope()) {
      var request = cm.getDropScope();
      if (request.hasOptions()) {
        var options = dropScopeOptions(request.getOptions(), spans);
        collections.dropScope(request.getName(), options);
      } else {
        collections.dropScope(request.getName());
      }
      setSuccess(result);
    } else if (cm.hasCreateCollection()) {
      var request = cm.getCreateCollection();
      var settings = createCollectionSettings(request.hasSettings(), request.getSettings());
      if (request.hasOptions()) {
        var options = createCollectionOptions(request.getOptions(), spans);
        collections.createCollection(request.getScopeName(), request.getName(), settings, options);
      } else {
        collections.createCollection(request.getScopeName(), request.getName(), settings);
      }
      setSuccess(result);
    } else if (cm.hasUpdateCollection()) {
      var request = cm.getUpdateCollection();
      var settings = updateCollectionSettings(request.hasSettings(), request.getSettings());
      if (request.hasOptions()) {
        var options = updateCollectionOptions(request.getOptions(), spans);
        collections.updateCollection(request.getScopeName(), request.getName(), settings, options);
      } else {
        collections.updateCollection(request.getScopeName(), request.getName(), settings);
      }
      setSuccess(result);
    } else if (cm.hasDropCollection()) {
      var request = cm.getDropCollection();
      if (request.hasOptions()) {
        var options = dropCollectionOptions(request.getOptions(), spans);
        collections.dropCollection(request.getScopeName(), request.getName(), options);
      } else {
        collections.dropCollection(request.getScopeName(), request.getName());
      }
      setSuccess(result);
    } else {
      throw new UnsupportedOperationException("Unknown CollectionManager operation");
    }
  }

  public static Mono<Result> handleCollectionManagerReactive(ReactiveCluster cluster,
                                                             ConcurrentHashMap<String, RequestSpan> spans,
                                                             com.couchbase.client.protocol.sdk.Command command,
                                                             Result.Builder result) {


    var cm = command.getBucketCommand().getCollectionManager();
    var bucketName = command.getBucketCommand().getBucketName();
    var collections = cluster.bucket(bucketName).collections();
    if (cm.hasGetAllScopes()) {
      Flux<com.couchbase.client.java.manager.collection.ScopeSpec> response;
      var request = cm.getGetAllScopes();
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
    } else if (cm.hasCreateScope()) {
      var request = cm.getCreateScope();
      Mono<Void> r;
      if (request.hasOptions()) {
        var options = createScopeOptions(request.getOptions(), spans);
        r = collections.createScope(request.getName(), options);
      } else {
        r = collections.createScope(request.getName());
      }
      return r.then(Mono.fromSupplier(() -> {
        setSuccess(result);
        return result.build();
      }));
    } else if (cm.hasDropScope()) {
      var request = cm.getDropScope();
      Mono<Void> r;
      if (request.hasOptions()) {
        var options = dropScopeOptions(request.getOptions(), spans);
        r = collections.dropScope(request.getName(), options);
      } else {
        r = collections.dropScope(request.getName());
      }
      return r.then(Mono.fromSupplier(() -> {
        setSuccess(result);
        return result.build();
      }));
    } else if (cm.hasCreateCollection()) {
      var request = cm.getCreateCollection();
      var settings = createCollectionSettings(request.hasSettings(), request.getSettings());
      Mono<Void> r;
      if (request.hasOptions()) {
        var options = createCollectionOptions(request.getOptions(), spans);
        r = collections.createCollection(request.getScopeName(), request.getName(), settings, options);
      } else {
        r = collections.createCollection(request.getScopeName(), request.getName(), settings);
      }
      return r.then(Mono.fromSupplier(() -> {
        setSuccess(result);
        return result.build();
      }));
    } else if (cm.hasUpdateCollection()) {
      var request = cm.getUpdateCollection();
      var settings = updateCollectionSettings(request.hasSettings(), request.getSettings());
      Mono<Void> r;
      if (request.hasOptions()) {
        var options = updateCollectionOptions(request.getOptions(), spans);
        r = collections.updateCollection(request.getScopeName(), request.getName(), settings, options);
      } else {
        r = collections.updateCollection(request.getScopeName(), request.getName(), settings);
      }
      return r.then(Mono.fromSupplier(() -> {
        setSuccess(result);
        return result.build();
      }));
    } else if (cm.hasDropCollection()) {
      var request = cm.getDropCollection();
      Mono<Void> r;
      if (request.hasOptions()) {
        var options = dropCollectionOptions(request.getOptions(), spans);
        r = collections.dropCollection(request.getScopeName(), request.getName(), options);
      } else {
        r = collections.dropCollection(request.getScopeName(), request.getName());
      }
      return r.then(Mono.fromSupplier(() -> {
        setSuccess(result);
        return result.build();
      }));
    } else {
      return Mono.error(new UnsupportedOperationException(new IllegalArgumentException("Unknown operation")));
    }
  }

  private static CollectionSpec.Builder toFit(com.couchbase.client.java.manager.collection.CollectionSpec spec) {
    CollectionSpec.Builder builder = CollectionSpec.newBuilder()
            .setName(spec.name())
            .setExpirySecs(spec.maxExpiry() != null ? (int) spec.maxExpiry().toSeconds() : 0)
            .setScopeName(spec.scopeName());
    if (spec.history() != null) {
      builder.setHistory(spec.history());
    }
    return builder;
  }

  public static void populateResult(Result.Builder result, List<com.couchbase.client.java.manager.collection.ScopeSpec> response) {
    var builder = GetAllScopesResult.newBuilder();

    var allResult = response.stream()
            .map(scopeSpec -> ScopeSpec.newBuilder()
                    .setName(scopeSpec.name())
                    .addAllCollections(scopeSpec.collections().stream()
                            .map(c -> toFit(c).build())
                            .collect(Collectors.toList()))
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
                    .map(c -> toFit(c).build())
                    .collect(Collectors.toList()))
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

  private static com.couchbase.client.java.manager.collection.CreateScopeOptions createScopeOptions(CreateScopeOptions options, ConcurrentHashMap<String, RequestSpan> spans) {
    var out = com.couchbase.client.java.manager.collection.CreateScopeOptions.createScopeOptions();

    if (options.hasTimeoutMsecs()) out.timeout(java.time.Duration.ofMillis(options.getTimeoutMsecs()));
    if (options.hasParentSpanId()) out.parentSpan(spans.get(options.getParentSpanId()));

    return out;
  }

  private static com.couchbase.client.java.manager.collection.DropScopeOptions dropScopeOptions(DropScopeOptions options, ConcurrentHashMap<String, RequestSpan> spans) {
    var out = com.couchbase.client.java.manager.collection.DropScopeOptions.dropScopeOptions();

    if (options.hasTimeoutMsecs()) out.timeout(java.time.Duration.ofMillis(options.getTimeoutMsecs()));
    if (options.hasParentSpanId()) out.parentSpan(spans.get(options.getParentSpanId()));

    return out;
  }

  private static com.couchbase.client.java.manager.collection.CreateCollectionOptions createCollectionOptions(CreateCollectionOptions options, ConcurrentHashMap<String, RequestSpan> spans) {
    var out = com.couchbase.client.java.manager.collection.CreateCollectionOptions.createCollectionOptions();

    if (options.hasTimeoutMsecs()) out.timeout(java.time.Duration.ofMillis(options.getTimeoutMsecs()));
    if (options.hasParentSpanId()) out.parentSpan(spans.get(options.getParentSpanId()));

    return out;
  }

  private static com.couchbase.client.java.manager.collection.CreateCollectionSettings createCollectionSettings(boolean hasSettings, CreateCollectionSettings settings) {
    var out = com.couchbase.client.java.manager.collection.CreateCollectionSettings.createCollectionSettings();

    if (hasSettings) {
      if (settings.hasExpirySecs()) out.maxExpiry(java.time.Duration.ofSeconds(settings.getExpirySecs()));
      if (settings.hasHistory()) out.history(settings.getHistory());
    }

    return out;
  }

  private static com.couchbase.client.java.manager.collection.UpdateCollectionOptions updateCollectionOptions(UpdateCollectionOptions options, ConcurrentHashMap<String, RequestSpan> spans) {
    var out = com.couchbase.client.java.manager.collection.UpdateCollectionOptions.updateCollectionOptions();

    if (options.hasTimeoutMsecs()) out.timeout(java.time.Duration.ofMillis(options.getTimeoutMsecs()));
    if (options.hasParentSpanId()) out.parentSpan(spans.get(options.getParentSpanId()));

    return out;
  }

  private static com.couchbase.client.java.manager.collection.UpdateCollectionSettings updateCollectionSettings(boolean hasSettings, UpdateCollectionSettings settings) {
    var out = com.couchbase.client.java.manager.collection.UpdateCollectionSettings.updateCollectionSettings();

    if (hasSettings) {
      if (settings.hasExpirySecs()) out.maxExpiry(java.time.Duration.ofSeconds(settings.getExpirySecs()));
      if (settings.hasHistory()) out.history(settings.getHistory());
    }

    return out;
  }

  private static com.couchbase.client.java.manager.collection.DropCollectionOptions dropCollectionOptions(DropCollectionOptions options, ConcurrentHashMap<String, RequestSpan> spans) {
    var out = com.couchbase.client.java.manager.collection.DropCollectionOptions.dropCollectionOptions();

    if (options.hasTimeoutMsecs()) out.timeout(java.time.Duration.ofMillis(options.getTimeoutMsecs()));
    if (options.hasParentSpanId()) out.parentSpan(spans.get(options.getParentSpanId()));

    return out;
  }

}
