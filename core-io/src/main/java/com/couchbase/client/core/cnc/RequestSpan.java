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
   * Sets an attribute on the span, which is translated to the corresponding implementation specific tag.
   * <p>
   * Note that, depending on the implementation, attributes might be ignored.
   *
   * @param key the key of the attribute.
   * @param value the value of the attribute.
   */
  void setAttribute(String key, String value);

  /**
   * Sets an event on the span, which is translated to the corresponding implementation specific event.
   * <p>
   * Note that, depending on the implementation, events might be ignored.
   *
   * @param name the name of the event
   * @param timestamp the timestamp when it happened.
   */
  void addEvent(String name, Instant timestamp);

  /**
   * Completes this span.
   *
   * @param tracer the tracer with the help of which it will be completed.
   */
  void end();

  /**
   * Allows to set a request context to the request span.
   *
   * @param requestContext the request context, if present.
   */
  @Stability.Internal
  void requestContext(RequestContext requestContext);

}
