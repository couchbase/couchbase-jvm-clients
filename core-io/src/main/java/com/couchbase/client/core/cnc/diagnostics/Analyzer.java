package com.couchbase.client.core.cnc.diagnostics;

import reactor.core.publisher.Mono;

public interface Analyzer {

  Mono<Void> start();

  Mono<Void> stop();

}
