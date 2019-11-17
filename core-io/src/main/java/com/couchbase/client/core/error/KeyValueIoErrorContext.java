package com.couchbase.client.core.error;

import com.couchbase.client.core.endpoint.EndpointContext;
import com.couchbase.client.core.msg.ResponseStatus;

public class KeyValueIoErrorContext extends ErrorContext {

  private final EndpointContext endpointContext;

  public KeyValueIoErrorContext(ResponseStatus responseStatus, EndpointContext endpointContext) {
    super(responseStatus);
    this.endpointContext = endpointContext;
  }

}
