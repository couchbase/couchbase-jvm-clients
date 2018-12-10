package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

public class InsertResponse extends BaseResponse {

  private final long cas;

  InsertResponse(ResponseStatus status, long cas) {
    super(status);
    this.cas = cas;
  }

  public long cas() {
    return cas;
  }
}
