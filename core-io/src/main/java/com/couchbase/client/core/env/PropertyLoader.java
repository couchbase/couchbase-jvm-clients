package com.couchbase.client.core.env;

public interface PropertyLoader<B extends CoreEnvironment.Builder> {

  void load(B builder);

}
