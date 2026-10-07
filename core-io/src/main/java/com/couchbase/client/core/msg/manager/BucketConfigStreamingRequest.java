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

package com.couchbase.client.core.msg.manager;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.io.netty.HttpProtocol;
import com.couchbase.client.core.retry.RetryStrategy;
import org.jspecify.annotations.Nullable;

import java.time.Duration;

/**
 * Performs a (potential endless) streaming request against the cluster manager for the given bucket.
 */
public class BucketConfigStreamingRequest extends BaseManagerRequest<BucketConfigStreamingResponse> {

  private static final String PATH = "/pools/default/bs/{}";

  private final String bucketName;

  public BucketConfigStreamingRequest(final Duration timeout, final CoreContext ctx,
                                      final RetryStrategy retryStrategy, final String bucketName) {
    super(timeout, ctx, retryStrategy);
    this.bucketName = bucketName;
  }

  @Override
  public BucketConfigStreamingResponse decode(int httpStatus, byte @Nullable [] content) {
    String lastDispatchedTo = null;
    if (context().lastDispatchedTo() != null) {
      lastDispatchedTo = context().lastDispatchedTo().host();
    }
    return new BucketConfigStreamingResponse(HttpProtocol.decodeStatus(httpStatus), lastDispatchedTo);
  }

  @Override
  public String path() {
    return CoreHttpPath.formatPath(PATH, bucketName);
  }

  @Override
  public boolean idempotent() {
    return true;
  }

}
