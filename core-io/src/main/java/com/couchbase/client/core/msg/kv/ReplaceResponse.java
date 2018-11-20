package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

public class ReplaceResponse extends BaseResponse {

  public ReplaceResponse(ResponseStatus status) {
    super(status);
  }
}
