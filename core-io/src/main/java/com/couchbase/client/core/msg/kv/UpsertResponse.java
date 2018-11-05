package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

public class UpsertResponse extends BaseResponse {

  public UpsertResponse(ResponseStatus status) {
    super(status);
  }
}
