/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.config.ProposedBucketConfigContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The {@link Refresher} is responsible to keep the configurations up to date after the initial
 * bootstrap/load.
 *
 * @since 1.0.0
 */
public interface Refresher {

  /**
   * Whenever a new config is loaded, it is pushed through this stream to be consumed.
   *
   * @return the stream of proposed configs.
   */
  Flux<ProposedBucketConfigContext> configs();

  /**
   * Registers a bucket for refreshing.
   *
   * @param name the name of the bucket.
   * @return a {@link Mono} once complete.
   */
  Mono<Void> register(String name);

  /**
   * Deregisters a bucket from refreshing (stopping the refresh).
   *
   * @param name the name of the bucket.
   * @return a {@link Mono} once complete.
   */
  Mono<Void> deregister(String name);

  /**
   * Marks the bucket as tainted, which will change the behavior of the refresher.
   *
   * <p>A config is marked as tainted during rebalance, which usually leads to shorter intervals
   * of checking if a new configuration exists (depending of the refresher impl).</p>
   *
   * @param name the name of the bucket.
   */
  void markTainted(String name);

  /**
   * Marks the bucket as untainted, which will change the behavior of the refresher.
   *
   * <p>A config is marked as tainted during rebalance, which usually leads to shorter intervals
   * of checking if a new configuration exists (depending of the refresher impl).</p>
   *
   * @param name the name of the bucket.
   */
  void markUntainted(String name);

  /**
   * Permanently shuts down the refresher.
   *
   * @return a {@link Mono} once complete.
   */
  Mono<Void> shutdown();

}
