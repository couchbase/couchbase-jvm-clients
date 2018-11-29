package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;

import java.util.HashMap;
import java.util.Map;

public class LookupSpec {

  private final Map<String, SpecType> specs;

  public static LookupSpec lookupSpec() {
    return new LookupSpec();
  }

  private LookupSpec() {
    this.specs = new HashMap<>();
  }

  public LookupSpec get(final String path) {
    specs.put(path, SpecType.GET);
    return this;
  }

  public LookupSpec exists(final String path) {
    specs.put(path, SpecType.EXISTS);
    return this;
  }

  @Stability.Internal
  public Map<String, SpecType> specs() {
    return specs;
  }

  enum SpecType {
    GET,
    EXISTS
  }

}
