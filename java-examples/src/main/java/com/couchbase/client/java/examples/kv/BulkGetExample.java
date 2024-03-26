/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.java.examples.kv;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.GetResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class BulkGetExample {
  public static void main(String... args) {
    Cluster cluster = Cluster.connect("127.0.0.1", "Administrator", "password");
    Bucket bucket = cluster.bucket("travel-sample");
    Collection collection = bucket.defaultCollection();

    Map<String, SuccessOrFailure<GetResult>> documentIdToResult = bulkGet(
      collection.reactive(),
      Arrays.asList( // or "List.of" if you're not stuck on Java 8
        "airline_10",
        "airport_1401",
        "hotel_33888",
        "bogus-document-id"
      )
    );

    documentIdToResult.forEach((documentId, result) -> {
        if (result.success != null) {
          GetResult r = result.success;
          System.out.println("Got '" + documentId + "' --> " + r);
        } else {
          Throwable t = result.failure;
          System.out.println("Failed to get '" + documentId + "' --> " + t);
        }
      }
    );
  }

  /**
   * Holds the result of an operation, either a success or a failure.
   * <p>
   * This class is a bare-bones version of Scala's "Try"
   * or Kotlin's "Result". Maybe one day Java will have something
   * like this in the standard library. We can dream!
   *
   * @param <T> the type for a successful value
   */
  public static final class SuccessOrFailure<T> {
    /**
     * The successful result, or null if this is a failure.
     */
    public final T success;

    /**
     * The failure, or null if this is a success.
     */
    public final Throwable failure;

    public static <T> SuccessOrFailure<T> success(T value) {
      return new SuccessOrFailure<>(requireNonNull(value), null);
    }

    public static <T> SuccessOrFailure<T> failure(Throwable t) {
      return new SuccessOrFailure<>(null, requireNonNull(t));
    }

    private SuccessOrFailure(T success, Throwable failure) {
      this.success = success;
      this.failure = failure;
    }

    @Override
    public String toString() {
      return success != null
        ? "success(" + success + ")"
        : "failure(" + failure + ")";
    }
  }

  /**
   * Efficient bulk get.
   *
   * @param collection The collection to get documents from.
   * @param documentIds The IDs of the documents to return.
   * @return a map (implementation unspecified)
   * where each given document ID is associated with the result of
   * getting the corresponding document from Couchbase.
   */
  public static Map<String, SuccessOrFailure<GetResult>> bulkGet(
    ReactiveCollection collection,
    Iterable<String> documentIds
  ) {
    // Delegate to the advanced version, with sensible defaults.
    return bulkGet(
      collection,
      documentIds,
      256,
      HashMap::new,
      Schedulers.immediate(),
      (documentId, value) -> value
    );
  }

  /**
   * Efficient bulk get, with advanced options.
   *
   * @param collection The collection to get documents from.
   * @param documentIds The IDs of the documents to return.
   * @param mapSupplier Factory for the returned map. Suggestion:
   * Pass {@code TreeMap::new} for sorted results,
   * or {@code HashMap::new} for unsorted.
   * @param concurrency Limits the number of Couchbases requests in flight
   * at the same time. Each invocation of this method has a separate quota.
   * Suggestion: Start with 256 and tune as desired.
   * @param mapValueTransformerScheduler The scheduler to use for converting
   * the result map values. Pass {@link Schedulers#immediate()}
   * to use the SDK's IO scheduler. Suggestion: If your value converter does IO,
   * pass {@link Schedulers#boundedElastic()}.
   * @param mapValueTransformer A function that takes a document ID and a result,
   * and returns the value to associated with that ID in the returned map.
   * @param <V> The return map's value type.
   * @param <M> The type of the map you'd like to store the results in.
   * @return a Map (implementation determined by {@code mapSupplier})
   * where each given document ID is associated with the result of
   * getting the corresponding document from Couchbase.
   */
  public static <V, M extends Map<String, V>> Map<String, V> bulkGet(
    ReactiveCollection collection,
    Iterable<String> documentIds,
    int concurrency,
    Supplier<M> mapSupplier,
    Scheduler mapValueTransformerScheduler,
    BiFunction<String, SuccessOrFailure<GetResult>, V> mapValueTransformer
  ) {
    return Flux.fromIterable(documentIds)
      .flatMap(
        // The `zip` operator associates things into a tuple;
        // in this case, a 2-tuple, commonly known as a "pair".
        //
        // Here the first element of the tuple is a document ID.
        // The second element is the result of getting the document
        // with that ID.
        //
        // Using a tuple lets us remember the association between
        // documentId + result as they flow through the reactive chain.
        documentId -> Mono.zip(
          Mono.just(documentId),
          collection.get(documentId)
            // Wrap the GetResult in a successful SuccessOrFailure
            .map(SuccessOrFailure::success)
            // Similarly, wrap the error in a failed SuccessOrFailure.
            // Logging is potentially IO; better to do it later
            // on a different scheduler.
            .onErrorResume(t -> Mono.just(SuccessOrFailure.failure(t)))
        ),
        concurrency
      )

      // Now we've got a flux of 2-tuples (pairs) where the
      // first element is the document ID, and the
      // second element is a `SuccessOrFailure` holding either
      // a GetResult (success) or a Throwable (failure).
      //
      // Next, collect the tuples into a map.
      // Each tuple becomes one map entry.
      //
      // Switch to the caller's scheduler before applying
      // the caller's value transformer.
      .publishOn(mapValueTransformerScheduler)
      .collect(
        mapSupplier,
        (map, idAndResult) -> {
          String documentId = idAndResult.getT1();
          SuccessOrFailure<GetResult> successOrFailure = idAndResult.getT2();
          map.put(documentId, mapValueTransformer.apply(documentId, successOrFailure));
        }
      )
      .block();
  }
}
