package com.couchbase.client.core.msg.manager;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

public class TerseBucketConfigResponse extends BaseResponse {

  private byte[] config;

  public TerseBucketConfigResponse(ResponseStatus status, byte[] config) {
    super(status);
    this.config = config;
  }

  public byte[] config() {
    return config;
  }
}
