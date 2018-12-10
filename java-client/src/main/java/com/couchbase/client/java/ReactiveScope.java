package com.couchbase.client.java;

import reactor.core.publisher.Mono;

import static com.couchbase.client.java.AsyncBucket.DEFAULT_COLLECTION;

public class ReactiveScope {

  private final String bucketName;
  private final AsyncScope asyncScope;

  ReactiveScope(AsyncScope asyncScope, String bucketName) {
    this.asyncScope = asyncScope;
    this.bucketName = bucketName;
  }

  public Mono<ReactiveCollection> defaultCollection() {
    return collection(DEFAULT_COLLECTION);
  }

  public Mono<ReactiveCollection> collection(final String name) {
    return Mono.fromFuture(asyncScope.collection(name))
      .map(asyncCollection -> new ReactiveCollection(asyncCollection, bucketName));
  }

}
