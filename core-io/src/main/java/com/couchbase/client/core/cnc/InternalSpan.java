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

/**
 * The {@link InternalSpan} tracks the nitty gritty details of the request/response cycle inside the SDK.
 * <p>
 * As the name suggests it is not intended to be used by users of the SDK, only actual implementors need to
 * worry about it. Users should look no further than the {@link RequestSpan} for passing in a parent span if
 * needed.
 */
@Stability.Internal
public interface InternalSpan {

  /**
   * Finishes the overall request span (does not touch or change any sub-spans).
   */
  void finish();

  /**
   * Called by the system once the request is created and gives the span a chance to look into request specific
   * information.
   *
   * @param ctx the request context once available.
   */
  void requestContext(RequestContext ctx);

  /**
   * Returns the request context so it can be accessed once set, usually by the tracer implementation.
   */
  RequestContext requestContext();

  /**
   * Signals the start of payload encoding, if needed for this request.
   */
  void startPayloadEncoding();

  /**
   * Signals the end of payload encoding, if needed for this request.
   */
  void stopPayloadEncoding();

  /**
   * Signals the start of the IO network dispatch phase for this request.
   */
  void startDispatch();

  /**
   * Signals the end of the IO network dispatch phase for this request.
   */
  void stopDispatch();

}
