package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

public class GetCollectionIdResponse extends BaseResponse {

  private final long collectionId;

  public GetCollectionIdResponse(final ResponseStatus status, final long collectionId) {
    super(status);
    this.collectionId = collectionId;
  }

  public long collectionId() {
    return collectionId;
  }

}
