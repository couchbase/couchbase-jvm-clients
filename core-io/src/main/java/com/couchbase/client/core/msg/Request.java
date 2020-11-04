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

package com.couchbase.client.core.msg;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a {@link Request} flowing through the client.
 *
 * @since 2.0.0
 */
public interface Request<R extends Response> {

  /**
   * Holds a unique ID for each request that is assigned automatically.
   *
   * @return the unique request ID.
   */
  long id();

  /**
   * Holds the response which eventually completes.
   *
   * @return the future containing the response, eventually.
   */
  CompletableFuture<R> response();

  /**
   * Completes this request successfully.
   *
   * @param result the result to complete with.
   */
  void succeed(R result);

  /**
   * Fails this request and completes it.
   *
   * @param error the error to fail this request with.
   */
  void fail(Throwable error);

  /**
   * Cancels this request.
   */
  void cancel(CancellationReason reason);

  /**
   * If attached, returns the context for this request.
   *
   * @return the request context if attached.
   */
  RequestContext context();

  /**
   * Returns the timeout for this request.
   *
   * @return the timeout for this request.
   */
  Duration timeout();

  /**
   * Returns true if the timeout elapsed.
   */
  boolean timeoutElapsed();

  /**
   * Allows to check if this request is completed already.
   *
   * @return true if completed, failed or cancelled.
   */
  boolean completed();

  /**
   * Allows to check if this request has been successfully completed.
   *
   * @return true if succeeded, false otherwise.
   */
  boolean succeeded();

  /**
   * Allows to check if this request has been completed but with an exception.
   *
   * @return true if failed, false otherwise.
   */
  boolean failed();

  /**
   * Allows to check if this request has been cancelled before it got a chance
   * to be either failed or succeeded.
   *
   * @return true if cancelled, false otherwise.
   */
  boolean cancelled();

  /**
   * If the request is {@link #cancelled()}, this returns the reason why.
   *
   * @return the cancellation reason if cancelled, null otherwise.
   */
  CancellationReason cancellationReason();

  /**
   * The service type of this request.
   *
   * @return the service type for this request.
   */
  ServiceType serviceType();

  /**
   * Returns contextual information for each individual service.
   *
   * @return the service context.
   */
  Map<String, Object> serviceContext();

  /**
   * The retry strategy for this particular request.
   *
   * @return the retry strategy for this request.
   */
  RetryStrategy retryStrategy();

  /**
   * Holds the internal span for this request.
   *
   * @return the internal span used for the request.
   */
  RequestSpan requestSpan();

  /**
   * Holds the nanoTime when the request got created.
   *
   * @return the time when the request got created.
   */
  long createdAt();

  /**
   * The absolute timestamp when the request should time out.
   *
   * @return the absolute timeout in nanoseconds.
   */
  long absoluteTimeout();

  /**
   * Returns if the given request is idempotent or not.
   *
   * <p>By default, this method always returns false for data consistency reasons. Only specific idempotent operations
   * should override this default since it impacts retry handling quite a bit. DO NOT SET THIS TO TRUE ON MUTATING
   * OPERATIONS!</p>
   *
   * @return true if idempotent.
   */
  default boolean idempotent() {
    return false;
  }

  /**
   * Returns a potentially non-unique identifier that is useful for tracing output.
   * <p>
   * Note: might be null! It depends on the type of operation. It is also different from the unqiue operation ID
   * that increments to provide additional context (i.e in query the context uuid, in kv the opaque value).
   *
   * @return if present, the operation id. Null otherwise.
   */
  default String operationId() {
    return null;
  }

  /**
   * The unique name of the request, usually related to the type but not necessarily.
   * <p>
   * The default implementation is derived from the class name (i.e. FooRequest => foo), but if that does
   * not match up it should be overridden in the actual class.
   *
   * @return the name of the request type.
   */
  default String name() {
    return getClass().getSimpleName().replace("Request", "").toLowerCase(Locale.ROOT);
  }

}
