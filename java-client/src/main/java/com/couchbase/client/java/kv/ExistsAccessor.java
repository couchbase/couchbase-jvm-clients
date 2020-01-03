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
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.GetMetaRequest;

import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.error.DefaultErrorUtil.keyValueStatusToException;

public class ExistsAccessor {

  private static ExistsResult CACHED_NOT_FOUND = new ExistsResult(false, 0);

  public static CompletableFuture<ExistsResult> exists(final String key, final Core core,
                                                       final GetMetaRequest request) {
    core.send(request);
    return request
      .response()
      .thenApply(response -> {
        if (response.status().success()) {
          return new ExistsResult(true, response.cas());
        } else if (response.status() == ResponseStatus.NOT_FOUND) {
          return CACHED_NOT_FOUND;
        }
        throw keyValueStatusToException(request, response);
      })
      .whenComplete((t, e) -> request.context().logicallyComplete());
  }

}
