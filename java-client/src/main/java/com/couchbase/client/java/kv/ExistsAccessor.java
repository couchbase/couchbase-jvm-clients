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
import com.couchbase.client.core.error.DefaultErrorUtil;
import com.couchbase.client.core.error.KeyNotFoundException;
import com.couchbase.client.core.msg.kv.ObserveViaCasRequest;
import com.couchbase.client.core.msg.kv.ObserveViaCasResponse;

import java.util.concurrent.CompletableFuture;

public class ExistsAccessor {

  public static CompletableFuture<ExistsResult> exists(final String key, final Core core,
                                                       final ObserveViaCasRequest request) {
    core.send(request);
    return request
      .response()
      .thenApply(response -> {
        boolean found = response.observeStatus() == ObserveViaCasResponse.ObserveStatus.FOUND_PERSISTED
          || response.observeStatus() == ObserveViaCasResponse.ObserveStatus.FOUND_NOT_PERSISTED;
        if (response.status().success() && found) {
            return new ExistsResult(response.cas());
        } else if (!found) {
          throw KeyNotFoundException.forKey(key);
        } else {
          throw DefaultErrorUtil.defaultErrorForStatus(key, response.status());
        }
      });
  }

}
