package com.couchbase.client.core.error;

import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.RequestContext;

import java.util.concurrent.CancellationException;

public class RequestCanceledException extends CancellationException {

  private final String name;
  private final RequestContext requestContext;

  public RequestCanceledException(String name, RequestContext context) {
    this.requestContext = context;
    this.name = name;
  }

  public String name() {
    return name;
  }

  public RequestContext requestContext() {
    return requestContext;
  }

  @Override
  public String getMessage() {
    return name + " " + requestContext.exportAsString(Context.ExportFormat.JSON);
  }
}
