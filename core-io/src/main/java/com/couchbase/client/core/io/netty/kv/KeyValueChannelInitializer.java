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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.CoreContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class KeyValueChannelInitializer extends ChannelInitializer<SocketChannel> {

  /**
   * Holds the core context as reference to event bus and more.
   */
  private final CoreContext coreContext;

  /**
   * The name of the bucket to connect.
   */
  private final String bucketName;

  /**
   * Name of the user.
   */
  private final String userName;

  /**
   * Password of the user.
   */
  private final String password;

  public KeyValueChannelInitializer(final CoreContext coreContext, final String bucketName,
                                    final String userName, final String password) {
    this.coreContext = coreContext;
    this.bucketName = bucketName;
    this.userName = userName;
    this.password = password;
  }

  @Override
  protected void initChannel(final SocketChannel ch) {
    ChannelPipeline pipeline = ch.pipeline();

    pipeline.addLast(new MemcacheProtocolDecodeHandler());
    pipeline.addLast(new MemcacheProtocolVerificationHandler(coreContext));

    pipeline.addLast(new FeatureNegotiatingHandler(coreContext, serverFeatures()));
    pipeline.addLast(new ErrorMapLoadingHandler(coreContext));

    if (!coreContext.environment().ioEnvironment().securityConfig().certAuthEnabled()) {
      pipeline.addLast(new SaslAuthenticationHandler(coreContext, userName, password));
    }

    pipeline.addLast(new SelectBucketHandler(coreContext, bucketName));

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
