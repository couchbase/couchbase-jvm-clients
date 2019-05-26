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

package com.couchbase.client.core.config;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.io.CollectionMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * The {@link ConfigurationProvider} is responsible for grabbing, converting and managing
 * bucket and cluster configurations.
 *
 * <p>This interface has been around since the 1.0 days, but it has been adapted to fit the
 * 2.x types and process.</p>
 *
 * @since 1.0.0
 */
@Stability.Internal
public interface ConfigurationProvider  {

  /**
   * This is a hot stream which when attached will return the current config as well as
   * all subsequent ones.
   *
   * @return a flux of new configurations as they arrive.
   */
  Flux<ClusterConfig> configs();

  /**
   * Returns the current {@link ClusterConfig}.
   *
   * @return the current cluster configuration.
   */
  ClusterConfig config();

  /**
   * Initiates the bucket opening process.
   *
   * <p>Note that when this mono completes, it does not mean that the process is completely
   * finished yet, just that it has been initiated and no hard error has been found at the
   * time.</p>
   *
   * @param name the name of the bucket to open.
   * @return a Mono that completes once the bucket has been logically opened.
   */
  Mono<Void> openBucket(String name);

  /**
   * Initiates the bucket closing process.
   *
   * @param name the name of the bucket.
   * @return a Mono that completes once the bucket has been logically closed.
   */
  Mono<Void> closeBucket(String name);

  /**
   * Shuts down the configuration provider and all its associated resources and timers.
   *
   * @return the mono completes once shut down properly.
   */
  Mono<Void> shutdown();

  /**
   * Allows to propose a bucket config to the provider from an external context.
   *
   * <p>This method is usually only called when a "not my vbucket" response is received and
   * the corresponding config is extracted. Do not call this method with arbitrary configs.</p>
   *
   * @param ctx the bucket config and surrounding context.
   */
  void proposeBucketConfig(ProposedBucketConfigContext ctx);

  /**
   * Returns the attached collection map.
   */
  CollectionMap collectionMap();

}
