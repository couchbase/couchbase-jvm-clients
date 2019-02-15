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
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.CouchbaseOutOfMemoryException;
import com.couchbase.client.core.error.TemporaryFailureException;
import com.couchbase.client.core.error.TemporaryLockFailureException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.kv.*;
import com.couchbase.client.core.service.kv.Observe;
import com.couchbase.client.core.service.kv.ObserveContext;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.util.CharsetUtil;
import io.opentracing.Span;
import io.opentracing.Tracer;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static com.couchbase.client.core.cnc.tracing.TracingUtils.completeSpan;

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
  public static CompletableFuture<Optional<GetResult>> get(final Core core, final String id,
                                                           final GetRequest request) {
    core.send(request);
    return request
      .response()
      .thenApply(getResponse -> {
        switch (getResponse.status()) {
          case SUCCESS:
            return Optional.of(GetResult.create(
              id,
              new EncodedDocument(getResponse.flags(), getResponse.content()),
              getResponse.cas(),
              Optional.empty()
            ));
          case NOT_FOUND:
            return Optional.<GetResult>empty();
          case TEMPORARY_FAILURE:
          case SERVER_BUSY:
            throw new TemporaryFailureException();
          case OUT_OF_MEMORY:
            throw new CouchbaseOutOfMemoryException();
          default:
            throw new CouchbaseException("Unexpected Status Code " + getResponse.status());
        }
    }).whenComplete((r, t) -> completeSpan(request));
  }

  public static CompletableFuture<Optional<GetResult>> getAndLock(final Core core, final String id,
                                                                  final GetAndLockRequest request) {
    core.send(request);
    return request
      .response()
      .thenApply(getResponse -> {
        switch (getResponse.status()) {
          case SUCCESS:
            return Optional.of(GetResult.create(
              id,
              new EncodedDocument(getResponse.flags(), getResponse.content()),
              getResponse.cas(),
              Optional.empty()
            ));
          case NOT_FOUND:
            return Optional.empty();
          case TEMPORARY_FAILURE:
          case LOCKED:
            throw new TemporaryLockFailureException();
          case SERVER_BUSY:
            throw new TemporaryFailureException();
          case OUT_OF_MEMORY:
            throw new CouchbaseOutOfMemoryException();
          default:
            throw new CouchbaseException("Unexpected Status Code " + getResponse.status());
        }
      });
  }

  public static CompletableFuture<Optional<GetResult>> getAndTouch(final Core core,
                                                                   final String id,
                                                                   final GetAndTouchRequest request,
                                                                   final PersistTo persistTo,
                                                                   final ReplicateTo replicateTo) {
    core.send(request);
    return request
      .response()
      .thenApply(getResponse -> {
        switch (getResponse.status()) {
          case SUCCESS:
            return Optional.of(GetResult.create(
              id,
              new EncodedDocument(getResponse.flags(), getResponse.content()),
              getResponse.cas(),
              Optional.empty()
            ));
          case NOT_FOUND:
            return Optional.empty();
          case TEMPORARY_FAILURE:
          case LOCKED:
          case SERVER_BUSY:
            throw new TemporaryFailureException();
          case OUT_OF_MEMORY:
            throw new CouchbaseOutOfMemoryException();
          default:
            throw new CouchbaseException("Unexpected Status Code " + getResponse.status());
        }
      }).thenCompose(r -> {
        Optional<GetResult> result = (Optional<GetResult>) r;
        if (!result.isPresent()) {
          return CompletableFuture.completedFuture(result);
        }

        final ObserveContext ctx = new ObserveContext(
          core.context(),
          persistTo.coreHandle(),
          replicateTo.coreHandle(),
          Optional.empty(), // GAT does NOT support mutation tokens.
          result.get().cas(),
          request.bucket(),
          id,
          request.collection(),
          false,
          request.timeout()
        );
        return Observe.poll(ctx).toFuture().thenApply(v -> result);
      });
  }

  public static CompletableFuture<Optional<GetResult>> subdocGet(final Core core, final String id,
                                                                 final SubdocGetRequest request) {
    core.send(request);
    return request
      .response()
      .thenApply(response -> {
        switch (response.status()) {
          case SUCCESS:
            return Optional.of(parseSubdocGet(id, response));
          case NOT_FOUND:
            return Optional.empty();
          default:
            throw new CouchbaseException("Unexpected Status Code " + response.status());
        }
      });
  }

  private static GetResult parseSubdocGet(final String id, final SubdocGetResponse response) {
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
      } catch (IOException e) {
        throw new CouchbaseException("Unexpected Exception while decoding Sub-Document get", e);
      }
    }

    Optional<Duration> expiration = exptime == null
      ? Optional.empty()
      : Optional.of(Duration.ofSeconds(Long.parseLong(new String(exptime, CharsetUtil.UTF_8))));

    // TODO: optimize the subdoc holder
    return GetResult.create(id, new EncodedDocument(0, content), cas, expiration);
  }

  /**
   * Helper method to recursively project subdocument fields into a json object structure.
   *
   * @param response the raw response from the server.
   * @return the document, encoded as a byte array.
   * @throws IOException if jackson gets a heart attack while parsing.
   */
  static byte[] projectRecursive(final SubdocGetResponse response) throws IOException {
    ObjectNode root = Mapper.mapper().createObjectNode();

    for (SubdocField value : response.values()) {
      if (value.status() != SubDocumentOpResponseStatus.SUCCESS
              || value.path().isEmpty()
              || EXPIRATION_MACRO.equals(value.path())) {
        continue;
      }

      String path = value.path();
      if (!path.contains(".")) {
        root.set(path, Mapper.mapper().readTree(value.value()));
        continue;
      }

      String[] pathComponents = path.split("\\.");
      ObjectNode parent = root;
      for (int i = 0; i < pathComponents.length - 1; i++) {
        String component = pathComponents[i];
        ObjectNode maybe = (ObjectNode) parent.get(component);
        if (maybe == null) {
          maybe = Mapper.mapper().createObjectNode();
          parent.set(component, maybe);
        }
        parent = maybe;
      }
      parent.set(
        pathComponents[pathComponents.length-1],
        Mapper.mapper().readTree(value.value())
      );
    }

    return Mapper.mapper().writeValueAsBytes(root);
  }

}
