package com.couchbase.client.core.error;

import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.KeyValueRequest;

import java.util.Map;

public class KeyValueErrorContext extends ErrorContext {

  private final KeyValueRequest<?> request;

  private KeyValueErrorContext(final KeyValueRequest<?> request, final ResponseStatus status) {
    super(status);
    this.request = request;
  }

  public static KeyValueErrorContext completedRequest(final KeyValueRequest<?> request, final ResponseStatus status) {
    return new KeyValueErrorContext(request, status);
  }

  public static KeyValueErrorContext incompleteRequest(final KeyValueRequest<?> request) {
    return new KeyValueErrorContext(request, null);
  }



  @Override
  public void injectExportableParams(final Map<String, Object> input) {
    super.injectExportableParams(input);
    if (request != null) {
      request.context().injectExportableParams(input);
    }
  }

}
