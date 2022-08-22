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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.msg.BaseResponse;
import com.couchbase.client.core.msg.ResponseStatus;

import java.util.Optional;

public class UpsertResponse extends KeyValueBaseResponse {

  final long cas;
  private final Optional<MutationToken> mutationToken;


  UpsertResponse(ResponseStatus status, long cas, Optional<MutationToken> mutationToken) {
    super(status);
    this.cas = cas;
    this.mutationToken = mutationToken;
  }

  public long cas() {
    return cas;
  }

  public Optional<MutationToken> mutationToken() {
    return mutationToken;
  }

  @Override
  public String toString() {
    return "UpsertResponse{" +
      "status=" + status() +
      ", cas=" + cas +
      ", mutationToken=" + mutationToken +
      '}';
  }
}
