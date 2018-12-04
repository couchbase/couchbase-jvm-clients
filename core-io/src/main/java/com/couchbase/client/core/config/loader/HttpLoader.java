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
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.service.ServiceType;
import reactor.core.publisher.Mono;

public class HttpLoader extends BaseLoader {

  public HttpLoader(final Core core) {
    super(core, ServiceType.CONFIG);
  }

  @Override
  protected Mono<String> discoverConfig(NetworkAddress seed, String bucket) {
    return null;
  }

  @Override
  protected int port() {
    return 0;
  }
}
