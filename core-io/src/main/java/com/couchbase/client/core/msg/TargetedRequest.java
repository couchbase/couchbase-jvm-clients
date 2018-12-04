package com.couchbase.client.core.msg;

import com.couchbase.client.core.io.NetworkAddress;

public interface TargetedRequest {

  NetworkAddress target();

}
