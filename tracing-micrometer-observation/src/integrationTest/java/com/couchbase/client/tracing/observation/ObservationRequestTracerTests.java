/*
 * Copyright (c) 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.tracing.observation;

import com.couchbase.client.core.cnc.RequestSpan;
import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleSpanInScope;
import io.micrometer.tracing.test.simple.SimpleTracer;
import io.micrometer.tracing.test.simple.SpansAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class ObservationRequestTracerTests {

  @Test
  void shouldTakeIntoConsiderationExplicitParentSpan() {
    SimpleTracer simpleTracer = new SimpleTracer();
    TestObservationRegistry testObservationRegistry = TestObservationRegistry.create();
    testObservationRegistry.observationConfig().observationHandler(new DefaultTracingObservationHandler(simpleTracer));
    ObservationRequestTracer tracer = ObservationRequestTracer.wrap(testObservationRegistry);

    SimpleSpan outsideOfCouchbase = simpleTracer.nextSpan().name("outside of couchbase").start();
    try (SimpleSpanInScope ws = simpleTracer.withSpan(outsideOfCouchbase)) {
      RequestSpan parent = tracer.requestSpan("parent", null);
      RequestSpan child = tracer.requestSpan("child", parent);
      child.end();
      parent.end();
    }
    outsideOfCouchbase.end();

    FinishedSpan outside = simpleTracer.getSpans().pop();
    FinishedSpan parent = simpleTracer.getSpans().pop();
    FinishedSpan child = simpleTracer.getSpans().pop();

    assertThat(outside.getName()).isEqualTo("outside of couchbase");
    assertThat(parent.getName()).isEqualTo("parent");
    assertThat(parent.getParentId()).isEqualTo(outside.getSpanId());
    assertThat(child.getName()).isEqualTo("child");
    assertThat(child.getParentId()).isEqualTo(parent.getSpanId());
  }
}
