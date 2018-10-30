package com.couchbase.client.core.error;

public class RequestCanceledException extends CouchbaseException {

  public RequestCanceledException() {
  }

  public RequestCanceledException(String message) {
    super(message);
  }

  public RequestCanceledException(String message, Throwable cause) {
    super(message, cause);
  }

  public RequestCanceledException(Throwable cause) {
    super(cause);
  }

  public RequestCanceledException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
