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

package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.BucketConfigParser;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Mono;

import java.util.Optional;

public abstract class BaseLoader implements Loader {

  private final Core core;
  private final ServiceType serviceType;

  BaseLoader(final Core core, final ServiceType serviceType) {
    this.core = core;
    this.serviceType = serviceType;
  }

  protected abstract Mono<String> discoverConfig(NetworkAddress seed, String bucket);

  @Override
  public Mono<BucketConfig> load(final NetworkAddress seed, final int port, final String name) {
    return core
      .ensureServiceAt(seed, serviceType, port, Optional.of(name))
      .then(discoverConfig(seed, name))
      .map(raw -> {
        String converted = raw.replace("$HOST", seed.address());
        return BucketConfigParser.parse(converted, core.context().environment(), seed);
      });
  }

  protected Core core() {
    return core;
  }
}
