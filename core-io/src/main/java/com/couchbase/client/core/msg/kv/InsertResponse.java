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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import reactor.util.annotation.Nullable;

import java.util.Optional;

import static com.couchbase.client.core.error.DefaultErrorUtil.keyValueStatusToException;

public class InsertResponse extends KeyValueBaseResponse {

  private final long cas;
  private final Optional<MutationToken> mutationToken;

  InsertResponse(ResponseStatus status, long cas, Optional<MutationToken> mutationToken, @Nullable MemcacheProtocol.FlexibleExtras flexibleExtras) {
    super(status, flexibleExtras);
    this.cas = cas;
    this.mutationToken = mutationToken;
  }

  public long cas() {
    return cas;
  }

  public Optional<MutationToken> mutationToken() {
    return mutationToken;
  }

  public RuntimeException errorIfNeeded(final InsertRequest request) {
    if (status() == ResponseStatus.EXISTS) {
      throw new DocumentExistsException(KeyValueErrorContext.completedRequest(request, this));
    }
    return keyValueStatusToException(request, this);
  }
}
