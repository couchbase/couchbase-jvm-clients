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

/**
 * Describes the reason why a {@link Request} has been cancelled.
 *
 * @since 2.0.0
 */
public enum CancellationReason {

  /**
   * The downstream consumer stopped listening for a result and therefore any further
   * processing is a waste of resources.
   */
  STOPPED_LISTENING,

  /**
   * The request ran into a timeout and is therefore cancelled before it got a chance
   * to complete.
   */
  TIMEOUT,

  /**
   * The user or some other code proactively cancelled the request by cancelling
   * it through its attached context.
   */
  CANCELLED_VIA_CONTEXT,

  /**
   * For a different reason. Make sure to emit an event so that debugging provides
   * further context.
   */
  OTHER

}
