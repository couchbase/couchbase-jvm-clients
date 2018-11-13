package com.couchbase.client.core;

import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;

public class Core {

  public <R extends Response> void send(final Request<R> request) {
    return;
  }

  public CoreContext context() {
    return null;
  }

}
