package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.error.subdoc.SubDocumentException;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import io.netty.util.CharsetUtil;

import java.util.List;
import java.util.Optional;

public class SubdocMutateResponse extends BaseResponse {

  private final List<SubdocField> values;
  private final long cas;
  private final Optional<MutationToken> mutationToken;
  private final Optional<SubDocumentException> error;

  public SubdocMutateResponse(ResponseStatus status,
                              Optional<SubDocumentException> error,
                              List<SubdocField> values,
                              long cas,
                              Optional<MutationToken> mutationToken) {
    super(status);
    this.error = error;
    this.values = values;
    this.cas = cas;
    this.mutationToken = mutationToken;
  }

  public List<SubdocField> values() {
    return values;
  }

  public long cas() {
    return cas;
  }

  public Optional<MutationToken> mutationToken() {
    return mutationToken;
  }

  /**
   * Error will be set, and should be checked and handled, when status==SUBDOC_FAILURE
   */
  public Optional<SubDocumentException> error() {
    return error;
  }

  @Override
  public String toString() {
    return "SubdocGetResponse{" +
      "values=" + values +
      ", cas=" + cas +
      '}';
  }
}
