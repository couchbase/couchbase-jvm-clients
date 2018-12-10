package com.couchbase.client.java;

import java.util.function.Function;

import static com.couchbase.client.java.AsyncUtils.block;

public class Scope {

  private final String bucketName;
  private final AsyncScope asyncScope;

  Scope(final AsyncScope asyncScope, final String bucketName) {
    this.asyncScope = asyncScope;
    this.bucketName = bucketName;
  }

  public Collection defaultCollection() {
    return block(asyncScope.defaultCollection()
      .thenApply(asyncCollection -> new Collection(asyncCollection, bucketName))
    );
  }

  public Collection collection(final String name) {
    return block(asyncScope.collection(name)
      .thenApply(asyncCollection -> new Collection(asyncCollection, bucketName))
    );
  }

}
