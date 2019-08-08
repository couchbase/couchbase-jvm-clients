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

package com.couchbase.client.core.config.loader;

import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.node.NodeIdentifier;
import reactor.core.publisher.Mono;

import java.util.Optional;

/**
 * The {@link BucketLoader} is responsible for initially loading a fresh configuration from the
 * cluster.
 *
 * @since 1.0.0
 */
public interface BucketLoader {

  /**
   * Attempts to load a config for the given seed node.
   *
   * <p>If something fails during the process, the error is propagated to the caller (i.e.
   * the service could not be enabled or fetching the config failed for some other reason
   * that was not recoverable in this loader specifically).</p>
   *
   * @param seed the seed node to attempt loading from.
   * @param bucket the name of the bucket.
   * @return a {@link Mono} eventually completing with a config or failing.
   */
  Mono<ProposedBucketConfigContext> load(final NodeIdentifier seed, int port, final String bucket,
                                         final Optional<String> alternateAddress);

}
