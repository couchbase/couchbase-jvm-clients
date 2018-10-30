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

package com.couchbase.client.core;

import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import reactor.core.publisher.Mono;

import java.util.concurrent.CancellationException;

/**
 * This class provides utility methods when working with reactor.
 *
 * @since 2.0.0
 */
public enum Reactor {
  ;

  /**
   * Wraps a {@link Request} and returns it in a {@link Mono}.
   *
   * @param request the request to wrap.
   * @param propagateCancellation if a cancelled/unsubscribed mono should also cancel the
   *                              request.
   * @return the mono that wraps the request.
   */
  public static <T extends Response> Mono<T> wrap(final Request<T> request,
                                                  final boolean propagateCancellation) {
    Mono<T> mono = Mono.fromFuture(request.response());
    if (propagateCancellation) {
      mono = mono
        .doFinally(st -> request.cancel(CancellationReason.STOPPED_LISTENING))
        // this is a workaround for https://github.com/reactor/reactor/issues/652
        .onErrorResume(e -> e instanceof CancellationException ? Mono.empty() : Mono.error(e));
    }
    return mono;
  }

}
