package com.couchbase.client.core.error;

public class CasMismatchException extends CouchbaseException {

  public CasMismatchException() {
  }

  public CasMismatchException(String message) {
    super(message);
  }

  public CasMismatchException(String message, Throwable cause) {
    super(message, cause);
  }

  public CasMismatchException(Throwable cause) {
    super(cause);
  }

  public CasMismatchException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
