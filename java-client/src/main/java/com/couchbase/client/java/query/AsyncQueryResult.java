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

package com.couchbase.client.java.query;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.java.codec.Decoder;
import com.couchbase.client.java.json.JacksonTransformers;
import com.couchbase.client.java.json.JsonObject;

public class AsyncQueryResult {

   private final CompletableFuture<QueryResponse> response;

   public AsyncQueryResult(CompletableFuture<QueryResponse> response) {
      this.response = response;
   }

   public CompletableFuture<String> requestId() {
      return this.response.thenCompose(r -> r.requestId().toFuture());
   }

   public CompletableFuture<String> clientContextId() {
      return this.response.thenCompose(r -> r.clientContextId().toFuture());
   }

   public CompletableFuture<QueryMetrics> info() {
      return this.response.thenCompose(r -> r.metrics().map(n -> {
         try {
            JsonObject jsonObject = JacksonTransformers.MAPPER.readValue(n, JsonObject.class);
            return new QueryMetrics(jsonObject);
         } catch (IOException ex) {
            throw new CouchbaseException(ex);
         }
      }).toFuture());
   }

   public CompletableFuture<String> queryStatus() {
      return this.response.thenCompose(r -> r.queryStatus().toFuture());
   }

   public CompletableFuture<List<JsonObject>> rows() {
      return this.response.thenCompose(r -> r.rows().map(n -> {
         try {
            return JacksonTransformers.MAPPER.readValue(n, JsonObject.class);
         } catch (IOException ex) {
            throw new CouchbaseException(ex);
         }
      }).collectList().toFuture());
   }

   public <T>CompletableFuture<List<T>> rows(Class<T> target, Decoder<T> decoder) {
      return this.response.thenCompose(r -> r.rows().map(n -> {
            try {
               return JacksonTransformers.MAPPER.readValue(n, target);
            } catch (IOException ex) {
               throw new CouchbaseException(ex);
            }
         }).collectList().toFuture());
   }

   public CompletableFuture<List<JsonObject>> warnings() {
      return this.response.thenCompose(r -> r.warnings().map(n -> {
         try {
            return JacksonTransformers.MAPPER.readValue(n, JsonObject.class);
         } catch (IOException ex) {
            throw new CouchbaseException(ex);
         }
      }).collectList().toFuture());
   }

   public CompletableFuture<List<JsonObject>> errors() {
      return this.response.thenCompose(r -> r.errors().map(n -> {
         try {
            return JacksonTransformers.MAPPER.readValue(n, JsonObject.class);
         } catch (IOException ex) {
            throw new IllegalStateException(ex);
         }
      }).collectList().toFuture());
   }
}