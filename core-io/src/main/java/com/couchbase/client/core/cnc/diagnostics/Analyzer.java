package com.couchbase.client.core.cnc.diagnostics;

import com.couchbase.client.core.cnc.Context;
import com.couchbase.client.core.cnc.Event;
import reactor.core.publisher.Mono;

public interface Analyzer {

  Event fetchEvent(Event.Severity severity, Context context);

  Mono<Void> start();

  Mono<Void> stop();

}
