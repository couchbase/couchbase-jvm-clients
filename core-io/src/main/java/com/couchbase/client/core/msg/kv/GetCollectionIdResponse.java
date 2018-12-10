package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

import java.util.Optional;

public class GetCollectionIdResponse extends BaseResponse {

  private final Optional<Long> collectionId;

  public GetCollectionIdResponse(final ResponseStatus status, final Optional<Long> collectionId) {
    super(status);
    this.collectionId = collectionId;
  }

  public Optional<Long> collectionId() {
    return collectionId;
  }

}
