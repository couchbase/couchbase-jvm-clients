/*
 * Copyright (c) 2021 Couchbase, Inc.
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

import com.couchbase.client.core.config.BucketCapabilities;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.Response;
import com.couchbase.client.core.msg.analytics.AnalyticsRequest;
import com.couchbase.client.core.service.ServiceType;

public class AnalyticsLocator extends RoundRobinLocator {

  public AnalyticsLocator() {
    super(ServiceType.ANALYTICS);
  }

  @Override
  protected boolean checkServiceNotAvailable(Request<? extends Response> request, ClusterConfig config) {
    if (request instanceof AnalyticsRequest) {
      AnalyticsRequest ar = (AnalyticsRequest) request;
      if (ar.scope() != null && ar.bucket() != null) {
        // This is a scope level query, so let's check if the cluster actually supports collections.
        BucketConfig bc = config.bucketConfig(ar.bucket());
        if (bc != null && !bc.bucketCapabilities().contains(BucketCapabilities.COLLECTIONS)) {
          request.fail(FeatureNotAvailableException.scopeLevelQuery(ServiceType.ANALYTICS));
          return false;
        }
      }
    }
    return true;
  }

}
