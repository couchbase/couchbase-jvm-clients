package com.couchbase.client.core.error;

public class StoppedListeningException extends RequestCanceledException {

  public static final StoppedListeningException INSTANCE = new StoppedListeningException();

  static {
    INSTANCE.setStackTrace(new StackTraceElement[] {});
  }

  public StoppedListeningException() {
  }

  public StoppedListeningException(String message) {
    super(message);
  }

  public StoppedListeningException(String message, Throwable cause) {
    super(message, cause);
  }

  public StoppedListeningException(Throwable cause) {
    super(cause);
  }

  public StoppedListeningException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
