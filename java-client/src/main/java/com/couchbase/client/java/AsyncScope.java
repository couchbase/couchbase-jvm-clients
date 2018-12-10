package com.couchbase.client.java;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.msg.kv.GetCollectionIdRequest;
import com.couchbase.client.java.env.ClusterEnvironment;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.java.AsyncBucket.DEFAULT_COLLECTION;
import static com.couchbase.client.java.AsyncBucket.DEFAULT_COLLECTION_ID;
import static com.couchbase.client.java.AsyncBucket.DEFAULT_SCOPE;

public class AsyncScope {


  private final Core core;
  private final String bucketName;
  private final String scopeName;
  private final ClusterEnvironment environment;

  AsyncScope(final String scopeName, final String bucketName, final Core core,
             final ClusterEnvironment environment) {
    this.scopeName = scopeName;
    this.bucketName = bucketName;
    this.core = core;
    this.environment = environment;
  }

  public String name() {
    return scopeName;
  }

  public CompletableFuture<AsyncCollection> defaultCollection() {
    return collection(DEFAULT_COLLECTION);
  }

  public CompletableFuture<AsyncCollection> collection(final String collection) {
    if (DEFAULT_COLLECTION.equals(collection) && DEFAULT_SCOPE.equals(scopeName)) {
      CompletableFuture<AsyncCollection> future = new CompletableFuture<>();
      future.complete(new AsyncCollection(collection, DEFAULT_COLLECTION_ID, scopeName,
        bucketName, core, environment));
      return future;
    } else {
      GetCollectionIdRequest request = new GetCollectionIdRequest(Duration.ofSeconds(1),
        core.context(), bucketName, environment.retryStrategy(), scopeName, collection);
      core.send(request);
      return request
        .response()
        .thenApply(res -> {
          if (res.status().success()) {
            return new AsyncCollection(collection, res.collectionId().get(), scopeName, bucketName,
              core, environment);
          } else {
            // TODO: delay into collection!
            throw new IllegalStateException("Do not raise me.. propagate into collection.. " +
              "collection error");
          }
        });
    }
  }


}
