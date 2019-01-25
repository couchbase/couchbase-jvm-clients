package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;
import io.netty.util.CharsetUtil;

import java.util.Arrays;
import java.util.List;

public class SubdocGetResponse extends BaseResponse {

  private final List<ResponseValue> values;
  private final long cas;

  public SubdocGetResponse(ResponseStatus status, List<ResponseValue> values, long cas) {
    super(status);
    this.values = values;
    this.cas = cas;
  }

  public List<ResponseValue> values() {
    return values;
  }

  public long cas() {
    return cas;
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
