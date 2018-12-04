package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

public class BucketConfigResponse extends BaseResponse {

  private final byte[] content;

  BucketConfigResponse(final ResponseStatus status, final byte[] content) {
    super(status);
    this.content = content;
  }

  public byte[] content() {
    return content;
  }

}
