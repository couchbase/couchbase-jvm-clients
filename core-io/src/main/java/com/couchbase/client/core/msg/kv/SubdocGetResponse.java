package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.error.subdoc.SubDocumentException;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

import java.util.List;
import java.util.Optional;

public class SubdocGetResponse extends BaseResponse {

  private final List<SubdocField> values;
  private final long cas;
  private final Optional<SubDocumentException> error;

  public SubdocGetResponse(ResponseStatus status, Optional<SubDocumentException> error, List<SubdocField> values, long cas) {
    super(status);
    this.error = error;
    this.values = values;
    this.cas = cas;
  }

  public List<SubdocField> values() {
    return values;
  }

  public long cas() {
    return cas;
  }

  /**
   * Error will be set, and should be checked and handled, when status==SUBDOC_FAILURE
   */
  public Optional<SubDocumentException> error() { return error; }

  @Override
  public String toString() {
    return "SubdocGetResponse{" +
      "error=" + error +
      "values=" + values +
      ", cas=" + cas +
      '}';
  }
}
