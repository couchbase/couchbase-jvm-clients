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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.error.*;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.UnlockRequest;

import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.error.DefaultErrorUtil.keyValueStatusToException;

public class UnlockAccessor {

  public static CompletableFuture<Void> unlock(final String key, final Core core, final UnlockRequest request) {
    core.send(request);
    return request
      .response()
      .thenApply(response -> {
        if (response.status().success()) {
          return null;
        } else if (response.status() == ResponseStatus.LOCKED) {
          throw new CasMismatchException(KeyValueErrorContext.completedRequest(request, response));
        }
        throw keyValueStatusToException(request, response);
      })
      .whenComplete((aVoid, throwable) -> request.context().logicallyComplete(throwable))
      // Don't ask, need this otherwise it won't compile (void vs. object)
      .thenApply(o -> null);
  }
}
