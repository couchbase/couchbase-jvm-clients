/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.node;

import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.MemcachedBucketConfig;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.ScopedRequest;
import com.couchbase.client.core.service.ServiceType;

/**
 * The {@link ViewLocator} extends the round-robin locator with some custom checks.
 */
public class ViewLocator extends RoundRobinLocator {

  public ViewLocator() {
    super(ServiceType.VIEWS);
  }

  @Override
  protected boolean checkServiceNotAvailable(Request<? extends Response> request, final ClusterConfig config) {
    if (request instanceof ScopedRequest) {
      String bucket = ((ScopedRequest) request).bucket();
      BucketConfig bucketConfig = config.bucketConfig(bucket);
      if (bucketConfig instanceof MemcachedBucketConfig) {
        request.fail(new FeatureNotAvailableException("Memcached buckets do not support view queries"));
        return false;
      }
      if (bucketConfig instanceof CouchbaseBucketConfig && ((CouchbaseBucketConfig) bucketConfig).ephemeral()) {
        request.fail(new FeatureNotAvailableException("Ephemeral buckets do not support view queries"));
        return false;
      }
    }
    return true;
  }

  /**
   * In addition to checking that the view service is enabled, for view dispatching it is vital that
   * a request is only ever sent to a node which has active primary KV partitions.
   *
   * @param node the node to check against.
   * @param request the request in scope.
   * @param config the cluster-level config.
   * @return true if the node can be used to dispatch the request.
   */
  @Override
  protected boolean nodeCanBeUsed(final Node node, final Request<? extends Response> request,
                                  final ClusterConfig config) {
    if (request instanceof ScopedRequest
      && config.bucketConfig(((ScopedRequest) request).bucket()) instanceof CouchbaseBucketConfig) {
        return ((CouchbaseBucketConfig) config.bucketConfig(((ScopedRequest) request).bucket()))
          .hasPrimaryPartitionsOnNode(node.identifier().address());
    }
    return false;
  }

}
