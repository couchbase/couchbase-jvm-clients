/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.msg.Request;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.tag.Tags;

import java.util.Optional;

public enum TracingUtils {
  ;

  public static void attachSpan(final CoreEnvironment env, final Optional<Span> parent,
                                final Request<?> request) {
    if (env.operationTracingEnabled()) {
      Scope scope = env.tracer()
        .buildSpan("get")
        .withTag(Tags.SPAN_KIND.getKey(), "client")
        .withTag(Tags.DB_TYPE.getKey(), "couchbase")
        .withTag(Tags.COMPONENT.getKey(), env.userAgent().formattedLong())
        .asChildOf(parent.orElse(null))
        .startActive(false);
      request.context().span(scope.span());
      scope.close();
    }
  }

  public static void completeSpan(final Request<?> request) {
    final Span span = request.context().span();
    if (span != null) {
      request
        .context()
        .environment()
        .tracer()
        .scopeManager()
        .activate(span, true)
        .close();
    }
  }

}
