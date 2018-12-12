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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Stability.Internal
public enum GetAccessor {
  ;

  /**
   * Takes a {@link GetRequest} and dispatches, converts and returns the result.
   *
   * @param core the core reference to dispatch into.
   * @param id the document id to fetch.
   * @param request the request to dispatch and convert once a response arrives.
   * @return a {@link CompletableFuture} once the document is fetched and decoded.
   */
  public static CompletableFuture<Optional<GetResult>> get(final Core core, final String id,
                                                           final GetRequest request) {
    core.send(request);
    return request
      .response()
      .thenApply(getResponse -> {
        if (getResponse.status().success()) {
          return Optional.of(GetResult.create(
            id,
            new EncodedDocument(0, getResponse.content()),
            getResponse.cas(),
            Optional.empty()
          ));
        } else if (getResponse.status() == ResponseStatus.NOT_FOUND) {
          return Optional.empty();
        } else {
          // todo: implement me
          throw new UnsupportedOperationException("fixme");
        }
      });
  }

  public static CompletableFuture<Optional<GetResult>> subdocGet(final Core core, final String id,
                                                                 final SubdocGetRequest request) {
    core.send(request);
    return request
      .response()
      .thenApply(subdocResponse -> {
        if (subdocResponse.status().success()) {
          long cas = subdocResponse.cas();

          try {
            // TODO: this is not very efficient and a hack - fix me
            ObjectNode node = Mapper.mapper().createObjectNode();
            for (SubdocGetResponse.ResponseValue value : subdocResponse.values()) {
              if (value.path().contains(".")) {
                throw new UnsupportedOperationException("nested paths are not working mapped yet");
              }
              node.set(value.path(), Mapper.mapper().readTree(value.value()));
            }

            return Optional.of(GetResult.create(
              id,
              new EncodedDocument(0, Mapper.mapper().writeValueAsBytes(node)),
              cas,
              Optional.empty())
            );
          } catch (IOException e) {
            // TODO: fixme
            throw new RuntimeException(e);
          }
        } else if (subdocResponse.status() == ResponseStatus.NOT_FOUND) {
            return Optional.empty();
        } else {
          // todo: implement me
          throw new UnsupportedOperationException("fixme");
        }
      });
  }


}
