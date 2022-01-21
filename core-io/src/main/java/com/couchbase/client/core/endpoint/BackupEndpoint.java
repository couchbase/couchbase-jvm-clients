/*
 * Copyright (c) 2022 Couchbase, Inc.
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

import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpClientCodec;
import com.couchbase.client.core.io.netty.NonChunkedHttpMessageHandler;
import com.couchbase.client.core.service.ServiceContext;
import com.couchbase.client.core.service.ServiceType;

public class BackupEndpoint extends BaseEndpoint {

  public BackupEndpoint(final ServiceContext ctx, final String hostname, final int port) {
    super(hostname, port, ctx.environment().ioEnvironment().backupEventLoopGroup().get(),
        ctx, ctx.environment().ioConfig().backupCircuitBreakerConfig(), ServiceType.BACKUP, false);
  }

  @Override
  protected PipelineInitializer pipelineInitializer() {
    return new BackupPipelineInitializer(context());
  }

  public static class BackupPipelineInitializer implements PipelineInitializer {

    private final EndpointContext endpointContext;

    public BackupPipelineInitializer(EndpointContext endpointContext) {
      this.endpointContext = endpointContext;
    }

    @Override
    public void init(BaseEndpoint endpoint, ChannelPipeline pipeline) {
      pipeline.addLast(new HttpClientCodec());
      pipeline.addLast(NonChunkedHttpMessageHandler.IDENTIFIER, new NonChunkedHttpMessageHandler(endpoint, ServiceType.BACKUP) {
      });
    }
  }
}
