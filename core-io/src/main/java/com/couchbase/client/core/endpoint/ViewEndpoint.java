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

import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.service.ServiceContext;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.core.deps.io.netty.channel.ChannelPipeline;
import com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpClientCodec;
import com.couchbase.client.core.deps.io.netty.handler.logging.LogLevel;
import com.couchbase.client.core.deps.io.netty.handler.logging.LoggingHandler;

public class ViewEndpoint extends BaseEndpoint {

  public ViewEndpoint(final ServiceContext ctx, final NetworkAddress hostname,
                      final int port) {
    super(hostname, port, ctx.environment().ioEnvironment().viewEventLoopGroup().get(),
      ctx, ctx.environment().ioConfig().viewCircuitBreakerConfig(), ServiceType.VIEWS);
  }

  @Override
  protected PipelineInitializer pipelineInitializer() {
    return new ViewPipelineInitializer();
  }

  public static class ViewPipelineInitializer implements PipelineInitializer {

    @Override
    public void init(ChannelPipeline pipeline) {
      pipeline.addLast(new LoggingHandler(LogLevel.ERROR));
      pipeline.addLast(new HttpClientCodec());
      //pipeline.addLast(new QueryMessageHandler());
    }
  }

}
