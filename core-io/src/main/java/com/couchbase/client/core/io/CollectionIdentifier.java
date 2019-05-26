package com.couchbase.client.core.io;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The {@link CollectionIdentifier} uniquely identifies the position of a collection.
 *
 * @since 2.0.0
 */
public class CollectionIdentifier {



  public static final String DEFAULT_SCOPE = "_default";
  public static final String DEFAULT_COLLECTION = "_default";

  private final String bucket;
  private final Optional<String> scope;
  private final Optional<String> collection;

  public static CollectionIdentifier fromDefault(String bucket) {
    return new CollectionIdentifier(bucket, Optional.of(DEFAULT_SCOPE), Optional.of(DEFAULT_COLLECTION));
  }

  public CollectionIdentifier(String bucket, Optional<String> scope, Optional<String> collection) {
    requireNonNull(bucket);
    requireNonNull(scope);
    requireNonNull(collection);

    this.bucket = bucket;
    this.scope = scope;
    this.collection = collection;
  }

  public String bucket() {
    return bucket;
  }

  public Optional<String> scope() {
    return scope;
  }

  public Optional<String> collection() {
    return collection;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CollectionIdentifier that = (CollectionIdentifier) o;
    return Objects.equals(bucket, that.bucket) &&
      Objects.equals(scope, that.scope) &&
      Objects.equals(collection, that.collection);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bucket, scope, collection);
  }

  @Override
  public String toString() {
    return "CollectionIdentifier{" +
      "bucket='" + bucket + '\'' +
      ", scope=" + scope +
      ", collection=" + collection +
      '}';
  }
}
