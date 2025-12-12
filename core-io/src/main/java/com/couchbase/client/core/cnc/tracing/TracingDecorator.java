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

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import reactor.util.annotation.Nullable;

import java.util.List;

/**
 * Decorates spans with the correct attributes, based on what ObservabilitySemanticConvention(s) the user has chosen.
 */
@Stability.Internal
public class TracingDecorator {
  private final TracingDecoratorImpl v0 = new TracingDecoratorImplV0();
  private final TracingDecoratorImpl v1 = new TracingDecoratorImplV1();
  private final boolean useV0;
  private final boolean useV1;

  public TracingDecorator(List<ObservabilitySemanticConvention> conventions) {
    if (conventions.isEmpty()) {
      this.useV0 = true;
      this.useV1 = false;
    } else if (conventions.contains(ObservabilitySemanticConvention.DATABASE_DUP)) {
      this.useV0 = true;
      this.useV1 = true;
    } else if (conventions.contains(ObservabilitySemanticConvention.DATABASE)) {
      this.useV0 = false;
      this.useV1 = true;
    } else {
      throw new IllegalArgumentException("Unknown observability convention: " + conventions);
    }
  }

  public void provideAttr(TracingAttribute attribute, RequestSpan span, String value) {
    if (useV0) {
      v0.provideAttr(attribute, span, value);
    }
    if (useV1) {
      v1.provideAttr(attribute, span, value);
    }
  }

  public void provideAttr(TracingAttribute attribute, RequestSpan span, long value) {
    if (useV0) {
      v0.provideAttr(attribute, span, value);
    }
    if (useV1) {
      v1.provideAttr(attribute, span, value);
    }
  }

  public void provideAttr(TracingAttribute attribute, RequestSpan span, boolean value) {
    if (useV0) {
      v0.provideAttr(attribute, span, value);
    }
    if (useV1) {
      v1.provideAttr(attribute, span, value);
    }
  }

  public void provideLowCardinalityAttr(TracingAttribute attribute, RequestSpan span, String value) {
    if (useV0) {
      v0.provideLowCardinalityAttr(attribute, span, value);
    }
    if (useV1) {
      v1.provideLowCardinalityAttr(attribute, span, value);
    }
  }

  public void provideQueryStatementIfSafe(TracingAttribute attribute, RequestSpan span, String value, boolean parametersUsed) {
    if (useV0) {
      v0.provideAttr(attribute, span, value);
    }
    if (useV1 && parametersUsed) {
      v1.provideAttr(attribute, span, value);
    }
  }


  public void provideCommonDispatchSpanAttributes(final RequestSpan span,
                                                  @Nullable final String localId,
                                                  @Nullable final String localHost,
                                                  final int localPort,
                                                  @Nullable final String remoteCanonicalHost,
                                                  final int remoteCanonicalPort,
                                                  @Nullable final String remoteActualHost,
                                                  final int remoteActualPort,
                                                  @Nullable final String operationId) {
    if (useV0) {
      v0.provideCommonDispatchSpanAttributes(span, localId, localHost, localPort, remoteCanonicalHost, remoteCanonicalPort,
        remoteActualHost, remoteActualPort, operationId);
    }
    if (useV1) {
      v1.provideCommonDispatchSpanAttributes(span, localId, localHost, localPort, remoteCanonicalHost, remoteCanonicalPort,
        remoteActualHost, remoteActualPort, operationId);
    }
  }

  public void provideManagerOrActualService(RequestSpan span, String actualService) {
    if (useV0) {
      v0.provideLowCardinalityAttr(TracingAttribute.SERVICE, span, v0.managerOrActualService(actualService));
    }
    if (useV1) {
      v1.provideLowCardinalityAttr(TracingAttribute.SERVICE, span, v1.managerOrActualService(actualService));
    }
  }
}
