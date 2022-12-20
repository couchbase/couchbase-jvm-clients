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
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.CollectionMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Set;

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
   * Returns a stream of seed node sets sourced from the server's global config or bucket config.
   * <p>
   * Only nodes running the KV service are present in the set.
   * <p>
   * This is a hot stream which when attached will return the current set of seed nodes
   * as well as all subsequent sets. The returned Flux does not emit any items until
   * the client has received at least one config from the server.
   *
   * @return a flux of new sets of seed nodes as they arrive.
   */
  Flux<Set<SeedNode>> seedNodes();

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
   * @param pushConfig whether this should result in a config being pushed.  Not needed during e.g. shutdown.
   * @return a Mono that completes once the bucket has been logically closed.
   */
  Mono<Void> closeBucket(String name, boolean pushConfig);

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
   * Allows to propose a global config to the provider from an external context.
   *
   * @param ctx the context with the global config.
   */
  void proposeGlobalConfig(ProposedGlobalConfigContext ctx);

  /**
   * Instructs the provider to try and load the global config, and then manage it.
   */
  Mono<Void> loadAndRefreshGlobalConfig();

  /**
   * Returns the attached collection map.
   */
  CollectionMap collectionMap();

  /**
   * Helper method to refresh the collection map for the given collection.
   *
   * @param identifier the identifier to refresh.
   * @return once refreshed completes the mono (or fails if error).
   */
  void refreshCollectionId(CollectionIdentifier identifier);

  /**
   * Returns true if an initial global config load attempt is in progress.
   *
   * @return true if it is in progress, false if not (done or failed).
   */
  boolean globalConfigLoadInProgress();

  /**
   * Returns true if a bucket config load attempt is in progress.
   *
   * @return true if in progress, false if not.
   */
  boolean bucketConfigLoadInProgress();

  /**
   * Returns true while a collection refresh is in progress at all.
   */
  boolean collectionRefreshInProgress();

  /**
   * Returns true if a collection refresh is in progress for the given identifier.
   *
   * @param identifier the collection identifier to check.
   */
  boolean collectionRefreshInProgress(CollectionIdentifier identifier);

  /**
   * Signals to the config provider that certain types of config refreshes failed and action might need to be taken.
   *
   * @param failure the type of config refresh failure.
   */
  void signalConfigRefreshFailed(ConfigRefreshFailure failure);

}
