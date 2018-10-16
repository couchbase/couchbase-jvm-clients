package com.couchbase.client.java.env;

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PropertyLoader;

public class CouchbaseEnvironment extends CoreEnvironment {

  private CouchbaseEnvironment(Builder builder) {
    super(builder);
  }

  public static CouchbaseEnvironment create() {
    return builder().build();
  }

  public static CouchbaseEnvironment.Builder builder() {
    return new Builder();
  }

  public static class Builder extends CoreEnvironment.Builder<Builder> {

    public Builder load(final CouchbasePropertyLoader loader) {
      loader.load(this);
      return this;
    }

    public CouchbaseEnvironment build() {
      return new CouchbaseEnvironment(this);
    }
  }
}
