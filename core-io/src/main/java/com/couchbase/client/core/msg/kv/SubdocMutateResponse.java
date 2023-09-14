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

import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocol;
import com.couchbase.client.core.msg.ResponseStatus;
import reactor.util.annotation.Nullable;

import java.util.Arrays;
import java.util.Optional;

import static com.couchbase.client.core.error.DefaultErrorUtil.keyValueStatusToException;

public class SubdocMutateResponse extends KeyValueBaseResponse {

  private final SubDocumentField[] values;
  private final long cas;
  private final Optional<MutationToken> mutationToken;
  private final Optional<CouchbaseException> error;

  public SubdocMutateResponse(ResponseStatus status,
                              Optional<CouchbaseException> error,
                              SubDocumentField[] values,
                              long cas,
                              Optional<MutationToken> mutationToken,
                              @Nullable MemcacheProtocol.FlexibleExtras flexibleExtras) {
    super(status, flexibleExtras);
    this.error = error;
    this.values = values == null ? new SubDocumentField[0] : values;
    this.cas = cas;
    this.mutationToken = mutationToken;
  }

  public SubDocumentField[] values() {
    return values;
  }

  public long cas() {
    return cas;
  }

  public Optional<MutationToken> mutationToken() {
    return mutationToken;
  }

  /**
   * A top-level exception that will be thrown from the mutateIn() call.
   */
  public Optional<CouchbaseException> error() {
    return error;
  }

  public CouchbaseException throwError(final SubdocMutateRequest request, final boolean insertDocument) {
    final KeyValueErrorContext ctx = KeyValueErrorContext.completedRequest(request, this);
    if (insertDocument
            && (status() == ResponseStatus.EXISTS || status() == ResponseStatus.NOT_STORED)) {
      return new DocumentExistsException(ctx);
    } else if (error().isPresent()) {
      return error().get();
    }
    // This should be superfluous now - if the op failed then error() should be set - but leaving as a fail-safe.
    return keyValueStatusToException(request, this);
  }

  @Override
  public String toString() {
    return "SubdocMutateResponse{" +
      "values=" + Arrays.asList(values) +
      ", cas=" + cas +
      '}';
  }
}
