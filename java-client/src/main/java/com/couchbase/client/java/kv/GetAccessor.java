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
import com.couchbase.client.core.error.*;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.*;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.java.codec.Transcoder;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;

@Stability.Internal
public enum GetAccessor {
  ;

  /**
   * Constant for the subdocument expiration macro.
   */
  public static final String EXPIRATION_MACRO = "$document.exptime";

  /**
   * Takes a {@link GetRequest} and dispatches, converts and returns the result.
   *
   * @param core the core reference to dispatch into.
   * @param id the document id to fetch.
   * @param request the request to dispatch and convert once a response arrives.
   * @return a {@link CompletableFuture} once the document is fetched and decoded.
   */
  public static CompletableFuture<GetResult> get(final Core core, final String id,
                                                 final GetRequest request, Transcoder transcoder) {
    core.send(request);
    return request
      .response()
      .thenApply(getResponse -> {
        if (getResponse.status() == ResponseStatus.SUCCESS) {
          return new GetResult(
            getResponse.content(),
            getResponse.flags(),
            getResponse.cas(),
            Optional.empty(),
            transcoder
          );
        }
        throw DefaultErrorUtil.defaultErrorForStatus(id, getResponse.status());
      });
  }

  public static CompletableFuture<GetResult> getAndLock(final Core core, final String id,
                                                        final GetAndLockRequest request, Transcoder transcoder) {
    core.send(request);
    return request
      .response()
      .thenApply(getResponse -> {
        switch (getResponse.status()) {
          case SUCCESS:
            return new GetResult(
              getResponse.content(),
              getResponse.flags(),
              getResponse.cas(),
              Optional.empty(),
              transcoder
            );
          // This is a special case on getAndLock for backwards compatibility (see MB-13087)
          case TEMPORARY_FAILURE:
            throw LockException.forKey(id);
          default:
            throw DefaultErrorUtil.defaultErrorForStatus(id, getResponse.status());
        }
      });
  }

  public static CompletableFuture<GetResult> getAndTouch(final Core core, final String id,
                                                         final GetAndTouchRequest request, Transcoder transcoder) {
    core.send(request);
    return request
      .response()
      .thenApply(getResponse -> {
        if (getResponse.status() == ResponseStatus.SUCCESS) {
          return new GetResult(
            getResponse.content(),
            getResponse.flags(),
            getResponse.cas(),
            Optional.empty(),
            transcoder
          );
        }
        throw DefaultErrorUtil.defaultErrorForStatus(id, getResponse.status());
      });
  }

  public static CompletableFuture<GetResult> subdocGet(final Core core, final String id, final SubdocGetRequest request,
                                                       final Transcoder transcoder) {
    core.send(request);
    return request
      .response()
      .thenApply(response -> {
        if (response.status() == ResponseStatus.SUCCESS) {
          return parseSubdocGet(response, transcoder);
        }
        throw DefaultErrorUtil.defaultErrorForStatus(id, response.status());
      });
  }

  private static GetResult parseSubdocGet(final SubdocGetResponse response, Transcoder transcoder) {
    if (response.error().isPresent()) {
      throw response.error().get();
    }

    long cas = response.cas();

    byte[] exptime = null;
    byte[] content = null;

    for (SubdocField value : response.values()) {
      if (EXPIRATION_MACRO.equals(value.path())) {
        exptime = value.value();
      } else if (value.path().isEmpty()) {
        content = value.value();
      }
    }

    if (content == null) {
      try {
        content = projectRecursive(response);
      } catch (Exception e) {
        throw new CouchbaseException("Unexpected Exception while decoding Sub-Document get", e);
      }
    }

    Optional<Duration> expiration = exptime == null
      ? Optional.empty()
      : Optional.of(Duration.ofSeconds(Long.parseLong(new String(exptime, UTF_8))));
    return new GetResult(content, CodecFlags.JSON_COMPAT_FLAGS, cas, expiration, transcoder);
  }

  /**
   * Helper method to recursively project subdocument fields into a json object structure.
   *
   * @param response the raw response from the server.
   * @return the document, encoded as a byte array.
   */
  static byte[] projectRecursive(final SubdocGetResponse response) {
    ObjectNode root = Mapper.createObjectNode();

    for (SubdocField value : response.values()) {
      if (value.status() != SubDocumentOpResponseStatus.SUCCESS
              || value.path().isEmpty()
              || EXPIRATION_MACRO.equals(value.path())) {
        continue;
      }

      String path = value.path();
      if (!path.contains(".")) {
        root.set(path, Mapper.decodeIntoTree(value.value()));
        continue;
      }

      String[] pathComponents = path.split("\\.");
      ObjectNode parent = root;
      for (int i = 0; i < pathComponents.length - 1; i++) {
        String component = pathComponents[i];
        ObjectNode maybe = (ObjectNode) parent.get(component);
        if (maybe == null) {
          maybe = Mapper.createObjectNode();
          parent.set(component, maybe);
        }
        parent = maybe;
      }
      parent.set(
        pathComponents[pathComponents.length-1],
        Mapper.decodeIntoTree(value.value())
      );
    }

    return Mapper.encodeAsBytes(root);
  }

}
