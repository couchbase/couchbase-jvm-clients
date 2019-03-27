package com.couchbase.client.core.msg.view;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.json.Mapper;

import java.util.HashMap;
import java.util.Map;

public class ViewError {

  private String error;
  private String reason;

  public ViewError(String error, String reason) {
    this.error = error;
    this.reason = reason;
  }

  public String error() {
    return error;
  }

  public String reason() {
    return reason;
  }

  @Stability.Internal
  public byte[] reassemble() {
    Map<String, String> converted = new HashMap<>(2);
    converted.put("error", error);
    converted.put("reason", reason);
    return Mapper.encodeAsBytes(converted);
  }

}
