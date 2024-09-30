/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.api;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.CoreResources;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreKvBinaryOps;
import com.couchbase.client.core.api.kv.CoreKvOps;
import com.couchbase.client.core.api.manager.CoreBucketAndScope;
import com.couchbase.client.core.api.manager.search.CoreSearchIndexManager;
import com.couchbase.client.core.api.query.CoreQueryOps;
import com.couchbase.client.core.api.search.CoreSearchOps;
import com.couchbase.client.core.diagnostics.ClusterState;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.manager.CoreBucketManagerOps;
import com.couchbase.client.core.manager.CoreCollectionManager;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.util.ConnectionString;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.CbCollections.setOf;

/**
 * Provides access to the various {@code Core*Ops} instances.
 */
@Stability.Internal
public interface CoreCouchbaseOps {
  CoreKvOps kvOps(CoreKeyspace keyspace);

  CoreKvBinaryOps kvBinaryOps(CoreKeyspace keyspace);

  CoreQueryOps queryOps();

  CoreSearchOps searchOps(@Nullable CoreBucketAndScope scope);

  CoreBucketManagerOps bucketManager();

  CoreCollectionManager collectionManager(String bucketName);

  CoreSearchIndexManager clusterSearchIndexManager();

  CoreSearchIndexManager scopeSearchIndexManager(CoreBucketAndScope scope);

  CoreEnvironment environment();

  CoreResources coreResources();

  CompletableFuture<Void> waitUntilReady(
      Set<ServiceType> serviceTypes,
      Duration timeout,
      ClusterState desiredState,
      @Nullable String bucketName
  );

  Mono<Void> shutdown(final Duration timeout);

  static CoreCouchbaseOps create(
      CoreEnvironment env,
      Authenticator authenticator,
      ConnectionString connectionString
  ) {
    switch (connectionString.scheme()) {
      case COUCHBASE2:
        return new CoreProtostellar(env, authenticator, connectionString);
      case COUCHBASE:
      case COUCHBASES:
        return Core.create(env, authenticator, connectionString);
      default:
        throw InvalidArgumentException.fromMessage("Unrecognized connection string scheme: " + connectionString.scheme());
    }
  }

  static void checkConnectionStringScheme(
      ConnectionString cs,
      ConnectionString.Scheme... validSchemes
  ) {
    Set<ConnectionString.Scheme> set = setOf(validSchemes);
    if (!set.contains(cs.scheme())) {
      throw new IllegalArgumentException("Expected connection string scheme to be one of " + set + " but got: " + cs.scheme());
    }
  }

  /**
   * @deprecated This method goes away after the Core / Protostellar refactor.
   * For now, it helps components that depend on Core fail with
   * `FeatureNotAvailableException` when Protostellar is used.
   */
  @Deprecated
  default Core asCore() {
    if (!(this instanceof Core)) {
      throw new FeatureNotAvailableException("Not yet supported when using couchbase2 protocol.");
    }
    return (Core) this;
  }
}
