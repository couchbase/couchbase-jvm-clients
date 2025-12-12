/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.client.core.cnc.tracing;

import org.jspecify.annotations.Nullable;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;

@Stability.Internal
public interface TracingDecoratorImpl {

  default void provideAttr(final TracingAttribute attribute, final RequestSpan span, final String value) {
    String attr = attributeName(attribute);
    if (attr != null) {
      span.attribute(attr, value);
    }
  }

  default void provideAttr(final TracingAttribute attribute, final RequestSpan span, final long value) {
    String attr = attributeName(attribute);
    if (attr != null) {
      span.attribute(attr, value);
    }
  }

  default void provideAttr(final TracingAttribute attribute, final RequestSpan span, final boolean value) {
    String attr = attributeName(attribute);
    if (attr != null) {
      span.attribute(attr, value);
    }
  }

  default void provideLowCardinalityAttr(final TracingAttribute attribute, final RequestSpan span, final String value) {
    String attr = attributeName(attribute);
    if (attr != null) {
      span.lowCardinalityAttribute(attr, value);
    }
  }

  String managerOrActualService(String actualService);

  String meterOperations();

  @Nullable
  String attributeName(final TracingAttribute attribute);

  default String requireAttributeName(final TracingAttribute attribute) {
    String attributeName = attributeName(attribute);
    if (attributeName == null) {
      throw new IllegalArgumentException("No attribute found for attribute: " + attribute);
    }
    return attributeName;
  }

  void provideCommonDispatchSpanAttributes(RequestSpan span,
                                           @Nullable String localId,
                                           @Nullable String localHost,
                                           int localPort,
                                           @Nullable String remoteCanonicalHost,
                                           int remoteCanonicalPort,
                                           @Nullable String remoteActualHost,
                                           int remoteActualPort,
                                           @Nullable String operationId);
}
