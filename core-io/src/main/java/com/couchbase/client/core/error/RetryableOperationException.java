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

package com.couchbase.client.core.error;

/**
 * This operation has returned a temporary error, and the application can retry it.
 *
 * Note that in some cases the operation may have succeeded despite the error - for instance, if
 * the server completed the operation but was unable to report that success.
 *
 * For this reason, the application must consider its error handling strategy, possibly on a
 * per-operation basis.  Some simple idempotent operations can simply be retried, others may need
 * a more sophisticated approach, possibly including using getFromReplica to ascertain the current
 * document content.
 */
public interface RetryableOperationException {
}
