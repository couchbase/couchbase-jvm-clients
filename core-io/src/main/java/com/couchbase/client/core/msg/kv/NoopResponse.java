package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

public class NoopResponse extends BaseResponse {

  public NoopResponse(ResponseStatus status) {
    super(status);
  }
}
