package com.couchbase.client.java;

import com.couchbase.client.java.json.JsonObject;

import java.util.function.Function;


/**
 * Experimental prototype for a different result type on fetch.
 */
public class GetResult {

  private final long cas;
  private final byte[] content;

  public GetResult(long cas, byte[] content) {
    this.cas = cas;
    this.content = content;
  }

  public long cas() {
    return cas;
  }

  public JsonObject content() {
    return content(null);
  }

  public JsonObject content(final String path) {
    return contentAs(path, JsonObject.class);
  }

   public <T> T contentAs(final Class<T> target) {
      return contentAs(null, target);
   }

  public <T> T contentAs(final String path, final Class<T> target) {
    return contentAs(path, target, null);
  }

  public <T> T contentAs(final String path, final Class<T> target, final Function<byte[], T> decoder) {
    return null;
  }

  public ResultPath path(final String path) {
    return new ResultPath(this);
  }

}
