package com.couchbase.client.core.msg.query;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

import java.util.Arrays;


public abstract class QueryResponse extends BaseResponse {

  private final QueryEventSubscriber subscriber;

  public QueryResponse(ResponseStatus status, QueryEventSubscriber subscriber) {
    super(status);
    this.subscriber = subscriber;
  }

  public abstract void request(long rows);

  public abstract void cancel();

  public QueryEventSubscriber subscriber() {
    return subscriber;
  }

  public enum QueryEventType {
    ROW
  }

  public interface QueryEventSubscriber {

    void onNext(QueryEvent row);

    void onComplete();

  }

  public static class QueryEvent {
    private final QueryEventType rowType;
    private final byte[] data;

    public QueryEvent(QueryEventType rowType, byte[] data) {
      this.rowType = rowType;
      this.data = data;
    }

    public QueryEventType rowType() {
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
