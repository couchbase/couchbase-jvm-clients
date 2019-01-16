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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public class ClusterManagerRefresher implements Refresher {

  private final DirectProcessor<ProposedBucketConfigContext> configs = DirectProcessor.create();
  private final FluxSink<ProposedBucketConfigContext> configsSink = configs.sink();

  private final Core core;

  public ClusterManagerRefresher(final ConfigurationProvider provider, final Core core) {
    this.core = core;
  }

  @Override
  public Mono<Void> register(final String name) {
    // TODO
    return Mono.empty();
  }

  @Override
  public Mono<Void> deregister(final String name) {
    // TODO
    return Mono.empty();
  }

  @Override
  public Mono<Void> shutdown() {
    // TODO
    return Mono.empty();
  }

  @Override
  public Flux<ProposedBucketConfigContext> configs() {
    return configs;
  }

  /**
   * No action needed when a config is marked as tainted for the cluster manager
   * refresher, since the server pushes new configs anyways during rebalance.
   *
   * @param name the name of the bucket.
   */
  @Override
  public void markTainted(String name) { }

  /**
   * No action needed when a config is marked as untainted for the cluster manager
   * refresher, since the server pushes new configs anyways during rebalance.
   *
   * @param name the name of the bucket.
   */
  @Override
  public void markUntainted(String name) { }

}
