package com.couchbase.client.core.msg.search;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

public class UpsertSearchIndexResponse extends BaseResponse {

  private final byte[] content;

  public UpsertSearchIndexResponse(ResponseStatus status, final byte[] content) {
    super(status);
    this.content = content;
  }
}
