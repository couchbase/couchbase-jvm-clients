/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.protostellar.kv;

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreCounterResult;
import com.couchbase.client.core.api.kv.CoreMutationResult;
import com.couchbase.client.protostellar.kv.v1.AppendResponse;
import com.couchbase.client.protostellar.kv.v1.DecrementResponse;
import com.couchbase.client.protostellar.kv.v1.IncrementResponse;
import com.couchbase.client.protostellar.kv.v1.MutationToken;
import com.couchbase.client.protostellar.kv.v1.PrependResponse;

import java.util.Optional;

/**
 * For converting Protostellar GRPC KV responses.
 */
@Stability.Internal
public class CoreProtostellarKvBinaryResponses {

  private CoreProtostellarKvBinaryResponses() {}

  public static CoreMutationResult convertResponse(CoreKeyspace keyspace, String key, AppendResponse response) {
    return CoreProtostellarKeyValueResponses.convertMutationResult(keyspace, key, response.getCas(),
        response.hasMutationToken() ? response.getMutationToken() : null);
  }

  public static CoreMutationResult convertResponse(CoreKeyspace keyspace, String key, PrependResponse response) {
    return CoreProtostellarKeyValueResponses.convertMutationResult(keyspace, key, response.getCas(),
        response.hasMutationToken() ? response.getMutationToken() : null);
  }

  public static CoreCounterResult convertResponse(CoreKeyspace keyspace, String key, IncrementResponse response) {
    return convertCounterResult(keyspace, key, response.getCas(), response.getContent(), response.getMutationToken());
  }

  public static CoreCounterResult convertResponse(CoreKeyspace keyspace, String key, DecrementResponse response) {
    return convertCounterResult(keyspace, key, response.getCas(), response.getContent(), response.getMutationToken());
  }

  static CoreCounterResult convertCounterResult(CoreKeyspace keyspace, String key, long cas, long content, MutationToken mt) {
    Optional<com.couchbase.client.core.msg.kv.MutationToken> mutationToken;
    if (mt != null) {
      mutationToken = Optional.of(new com.couchbase.client.core.msg.kv.MutationToken((short) mt.getVbucketId(), mt.getVbucketId(), mt.getSeqNo(), mt.getBucketName()));
    } else {
      mutationToken = Optional.empty();
    }
    return new CoreCounterResult(null, keyspace, key, cas, content, mutationToken);
  }
}
