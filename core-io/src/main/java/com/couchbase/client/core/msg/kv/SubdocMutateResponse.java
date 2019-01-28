package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import io.netty.util.CharsetUtil;

import java.util.List;
import java.util.Optional;

public class SubdocMutateResponse extends BaseResponse {

  private final List<ResponseValue> values;
  private final long cas;
  private final Optional<MutationToken> mutationToken;

  public SubdocMutateResponse(ResponseStatus status, List<ResponseValue> values, long cas,
                              Optional<MutationToken> mutationToken) {
    super(status);
    this.values = values;
    this.cas = cas;
    this.mutationToken = mutationToken;
  }

  public List<ResponseValue> values() {
    return values;
  }

  public long cas() {
    return cas;
  }

  public Optional<MutationToken> mutationToken() {
    return mutationToken;
  }

  public static class ResponseValue {
    private final short status;
    private final byte[] value;
    private final String path;

    public ResponseValue(short status, byte[] value, String path) {
      this.status = status;
      this.value = value;
      this.path = path;
    }

    public short status() {
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
      "values=" + values +
      ", cas=" + cas +
      '}';
  }
}
