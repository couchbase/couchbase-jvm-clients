package com.couchbase.client.core.env;

import com.couchbase.client.core.cnc.EventBus;

public interface CoreEnvironment {

    String userAgent();

    EventBus eventBus();
}
