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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.io.NetworkAddress;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class DefaultConfigurationProvider implements ConfigurationProvider {

  /**
   * Link to the attached core.
   */
  private final Core core;

  /**
   * Holds the seed nodes to bootstrap from.
   */
  private final Set<NetworkAddress> seedNodes;

  /**
   * This stream sends new config updates to everyone interested.
   */
  private final EmitterProcessor<ClusterConfig> configStream;

  /**
   * Creates a new {@link DefaultConfigurationProvider}.
   *
   * @param core the attached core.
   * @param seedNodes the seed nodes to bootstrap from.
   * @return the initialized config provider.
   */
  public static DefaultConfigurationProvider create(final Core core,
                                                    final Set<NetworkAddress> seedNodes) {
    return new DefaultConfigurationProvider(core, seedNodes);
  }

  /**
   * Creates a new {@link DefaultConfigurationProvider}.
   *
   * @param core the attached core.
   * @param seedNodes the seed nodes to bootstrap from.
   */
  private DefaultConfigurationProvider(final Core core, final Set<NetworkAddress> seedNodes) {
    this.core = core;
    this.seedNodes = shuffle(seedNodes);
    this.configStream = EmitterProcessor.create();
  }

  /**
   * Helper method to shuffle the seed nodes.
   *
   * @param input the input seed node list.
   * @return the shuffled network addresses.
   */
  private Set<NetworkAddress> shuffle(final Set<NetworkAddress> input) {
    final List<NetworkAddress> hostsList = new ArrayList<>(input);
    Collections.shuffle(hostsList);
    return new LinkedHashSet<>(hostsList);
  }

  @Override
  public Flux<ClusterConfig> configs() {
    return configStream;
  }

  @Override
  public Mono<Void> openBucket(String name) {
    return null;
  }

  @Override
  public Mono<Void> closeBucket(String name) {
    return null;
  }

}
