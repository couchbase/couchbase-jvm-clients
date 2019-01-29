package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.error.subdoc.SubDocumentException;
import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import io.netty.util.CharsetUtil;

import java.util.List;
import java.util.Optional;

public class SubdocGetResponse extends BaseResponse {

  private final List<ResponseValue> values;
  private final long cas;
  private final Optional<SubDocumentException> error;

  public SubdocGetResponse(ResponseStatus status, Optional<SubDocumentException> error,  List<ResponseValue> values, long cas) {
    super(status);
    this.error = error;
    this.values = values;
    this.cas = cas;
  }

  public List<ResponseValue> values() {
    return values;
  }

  public long cas() {
    return cas;
  }

  /**
   * Error will be set, and should be checked and handled, when status==SUBDOC_FAILURE
   */
  public Optional<SubDocumentException> error() { return error; }

  public static class ResponseValue {
    private final SubDocumentResponseStatus status;
    private final byte[] value;
    private final String path;

    public ResponseValue(SubDocumentResponseStatus status, byte[] value, String path) {
      this.status = status;
      this.value = value;
      this.path = path;
    }

    public SubDocumentResponseStatus status() {
      return status;
    }

    public byte[] value() {
      return value;
    }

    public String path() {
      return path;
    }

    @Override
    public String toString() {
      return "ResponseValue{" +
        "status=" + status +
        ", value=" + new String(value, CharsetUtil.UTF_8) +
        ", path='" + path + '\'' +
        '}';
    }
  }

  @Override
  public String toString() {
    return "SubdocGetResponse{" +
      "error=" + error +
      "values=" + values +
      ", cas=" + cas +
      '}';
  }
}
