package com.couchbase.client.core.msg.query;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

import java.util.Arrays;
import java.util.function.Consumer;


public abstract class QueryResponse extends BaseResponse {

  private final Consumer<Row> consumer;

  public QueryResponse(ResponseStatus status, Consumer<Row> consumer) {
    super(status);
    this.consumer = consumer;
  }

  public abstract void request(long rows);

  public Consumer<Row> consumer() {
    return consumer;
  }

  public enum RowType {
    ROW,
    END
  }

  public static class Row {
    private final RowType rowType;
    private final byte[] data;

    public Row(RowType rowType, byte[] data) {
      this.rowType = rowType;
      this.data = data;
    }

    public RowType rowType() {
      return rowType;
    }

    public byte[] data() {
      return data;
    }

    @Override
    public String toString() {
      return "Row{" +
        "rowType=" + rowType +
        ", data=" + Arrays.toString(data) +
        '}';
    }
  }

}
