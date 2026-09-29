/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.service;

import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Wraps a {@link RequestBody} and declares it one-shot, so OkHttp will not retransmit it after the
 * exchange has started (stale-connection retries, 307/308, 401/407, 408, 421, 503 follow-ups).
 * Connect-phase failures are still retried across routes.
 * <p>
 * The wrapped body is not actually made single-use: {@link #writeTo} may be called more than
 * once (e.g. by a signing interceptor) and simply delegates each time.
 */
@NullMarked
public final class OneShotRequestBody extends RequestBody {
  private final RequestBody wrapped;

  private OneShotRequestBody(RequestBody wrapped) {
    this.wrapped = requireNonNull(wrapped);
  }

  /**
   * Creates a one-shot request body from the given byte array and content type.
   * <p>
   * The array is not copied; do not mutate it while the call is in flight.
   */
  public static OneShotRequestBody createOneShot(byte[] content, @Nullable MediaType contentType) {
    return new OneShotRequestBody(RequestBody.create(content, contentType));
  }

  @Override
  public boolean isOneShot() {
    return true;
  }

  @Override
  public @Nullable MediaType contentType() {
    return wrapped.contentType();
  }

  @Override
  public long contentLength() throws IOException {
    return wrapped.contentLength();
  }

  @Override
  public void writeTo(BufferedSink bufferedSink) throws IOException {
    wrapped.writeTo(bufferedSink);
  }

  @Override
  public boolean isDuplex() {
    return wrapped.isDuplex();
  }
}
