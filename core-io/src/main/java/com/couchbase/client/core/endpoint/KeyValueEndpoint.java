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

package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.env.Credentials;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.io.netty.kv.ErrorMapLoadingHandler;
import com.couchbase.client.core.io.netty.kv.FeatureNegotiatingHandler;
import com.couchbase.client.core.io.netty.kv.KeyValueMessageHandler;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocolDecodeHandler;
import com.couchbase.client.core.io.netty.kv.MemcacheProtocolVerificationHandler;
import com.couchbase.client.core.io.netty.kv.SaslAuthenticationHandler;
import com.couchbase.client.core.io.netty.kv.SelectBucketHandler;
import com.couchbase.client.core.io.netty.kv.ServerFeature;
import io.netty.channel.ChannelPipeline;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class KeyValueEndpoint extends BaseEndpoint {

  private final CoreContext coreContext;
  private final String bucketname;
  private final Credentials credentials;

  public KeyValueEndpoint(final CoreContext coreContext, final NetworkAddress hostname,
                          final int port, final String bucketname, final Credentials credentials) {
    super(hostname, port, coreContext.environment().ioEnvironment().kvEventLoopGroup().get(),
      coreContext, coreContext.environment().ioEnvironment().kvCircuitBreakerConfig());
    this.coreContext = coreContext;
    this.credentials = credentials;
    this.bucketname = bucketname;
  }

  @Override
  protected PipelineInitializer pipelineInitializer() {
    return new KeyValuePipelineInitializer(coreContext, bucketname, credentials);
  }

  public static class KeyValuePipelineInitializer implements PipelineInitializer {

    private final CoreContext coreContext;
    private final String bucketname;
    private final Credentials credentials;

    public KeyValuePipelineInitializer(CoreContext coreContext, String bucketname, Credentials credentials) {
      this.coreContext = coreContext;
      this.credentials = credentials;
      this.bucketname = bucketname;
    }

    @Override
    public void init(ChannelPipeline pipeline) {
      pipeline.addLast(new MemcacheProtocolDecodeHandler());
      pipeline.addLast(new MemcacheProtocolVerificationHandler(coreContext));

      pipeline.addLast(new FeatureNegotiatingHandler(coreContext, serverFeatures()));
      pipeline.addLast(new ErrorMapLoadingHandler(coreContext));

      if (!coreContext.environment().ioEnvironment().securityConfig().certAuthEnabled()) {
        pipeline.addLast(new SaslAuthenticationHandler(
          coreContext,
          credentials.usernameForBucket(bucketname),
          credentials.passwordForBucket(bucketname)
        ));
      }

      pipeline.addLast(new SelectBucketHandler(coreContext, bucketname));
      pipeline.addLast(new KeyValueMessageHandler(coreContext));
    }

    /**
     * Returns the server features that should be negotiated.
     *
     * @return the server features to negotiate.
     */
    private Set<ServerFeature> serverFeatures() {
      Set<ServerFeature> features = new HashSet<>(Arrays.asList(
        ServerFeature.SELECT_BUCKET,
        ServerFeature.XATTR,
        ServerFeature.XERROR
      ));

      if (coreContext.environment().ioEnvironment().compressionConfig().enabled()) {
        features.add(ServerFeature.SNAPPY);
      }

      return features;
    }
  }

}
