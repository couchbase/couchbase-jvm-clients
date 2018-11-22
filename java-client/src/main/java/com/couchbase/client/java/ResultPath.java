package com.couchbase.client.java;

import com.couchbase.client.java.json.JsonObject;

public class ResultPath {

  private final GetResult getResult;

  ResultPath(GetResult getResult) {
    this.getResult = getResult;
  }

  public JsonObject content() {
    return contentAs(JsonObject.class);
  }

  public <T> T contentAs(final Class<T> target) {
    return null;
  }


}
