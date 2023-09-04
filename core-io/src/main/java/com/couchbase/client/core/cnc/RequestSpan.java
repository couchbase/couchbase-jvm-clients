/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.cnc;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.RequestContext;

import java.time.Instant;

/**
 * Wrapper interface for all spans flowing through the SDK.
 * <p>
 * Note that you'll most likely consume this interface through actual implementations from the tracer module that
 * is used for your application. You will not need to worry about this with the default threshold request tracer, but
 * if you are using OpenTracing or OpenTelemetry, look in their respective modules for implementations of this class.
 */
@Stability.Volatile
public interface RequestSpan {

  /**
   * Sets a high-cardinality attribute on the span, which is translated to the corresponding implementation specific tag.
   * <p>
   * Note that, depending on the implementation, attributes might be ignored.
   *
   * @param key the key of the attribute.
   * @param value the value of the attribute.
   */
  void attribute(String key, String value);

  /**
   * Sets a high-cardinality attribute on the span, which is translated to the corresponding implementation specific tag.
   * <p>
   * Note that, depending on the implementation, attributes might be ignored.
   *
   * @param key the key of the attribute.
   * @param value the value of the attribute.
   */
  void attribute(String key, boolean value);

  /**
   * Sets a high-cardinality attribute on the span, which is translated to the corresponding implementation specific tag.
   * <p>
   * Note that, depending on the implementation, attributes might be ignored.
   *
   * @param key the key of the attribute.
   * @param value the value of the attribute.
   */
  void attribute(String key, long value);

  /**
   * Sets a low-cardinality attribute on the span, which is translated to the corresponding implementation specific tag.
   * <p>
   * Note that, depending on the implementation, attributes might be ignored.
   *
   * @param key the key of the attribute.
   * @param value the value of the attribute.
   */
  @Stability.Internal
  default void lowCardinalityAttribute(String key, String value) {
    attribute(key, value);
  }

  /**
   * Sets a low-cardinality attribute on the span, which is translated to the corresponding implementation specific tag.
   * <p>
   * Note that, depending on the implementation, attributes might be ignored.
   *
   * @param key the key of the attribute.
   * @param value the value of the attribute.
   */
  @Stability.Internal
  default void lowCardinalityAttribute(String key, boolean value) {
    attribute(key, value);
  }

  /**
   * Sets a low-cardinality attribute on the span, which is translated to the corresponding implementation specific tag.
   * <p>
   * Note that, depending on the implementation, attributes might be ignored.
   *
   * @param key the key of the attribute.
   * @param value the value of the attribute.
   */
  @Stability.Internal
  default void lowCardinalityAttribute(String key, long value) {
    attribute(key, value);
  }

  /**
   * Sets an event on the span, which is translated to the corresponding implementation specific event.
   * <p>
   * Note that, depending on the implementation, events might be ignored.
   *
   * @param name the name of the event
   * @param timestamp the timestamp when it happened.
   */
  void event(String name, Instant timestamp);

  /**
   * Sets the status of the span, which is by default UNSET.
   * <p>
   * Note that, depending on the implementation, this might be a no-op.
   *
   * @param status the span's new status.
   */
  void status(StatusCode status);

  /**
   * Records that an exception happened on the span.  What this does is dependent on the underlying telemetry
   * implementation - in some implementations it is a no-op.
   * <p>
   * The span still needs to have `span.end()` called.  And the application may want to set the status to StatusCode.ERROR.
   * Neither is done automatically, since it's possible for a span to have an exception but then recover.
   *
   * @param err the exception to record.
   */
  @Stability.Volatile
  default void recordException(Throwable err) {
  }

  /**
   * Completes this span.
   */
  void end();

  /**
   * Allows to set a request context to the request span.
   *
   * @param requestContext the request context, if present.
   */
  @Stability.Internal
  void requestContext(RequestContext requestContext);

  /**
   * Provides an abstraction over underlying tracing status codes.
   */
  enum StatusCode {
      UNSET,
      OK,
      ERROR;
  }
}
