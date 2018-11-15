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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The {@link ConfigurationProvider} is responsible for grabbing, converting and managing
 * bucket and cluster configurations.
 *
 * @since 1.0.0
 */
public interface ConfigurationProvider  {

  /**
   * This is a hot stream which when attached will return the current config as well as
   * all subsequent ones.
   *
   * @return a flux of new configurations as they arrive.
   */
  Flux<ClusterConfig> configs();

  Mono<Void> openBucket(final String name);

  Mono<Void> closeBucket(final String name);
}
