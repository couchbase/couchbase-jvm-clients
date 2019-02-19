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
import com.couchbase.client.core.io.netty.query.QueryMessageHandler;
import com.couchbase.client.core.service.ServiceContext;
import com.couchbase.client.core.service.ServiceType;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import reactor.core.scheduler.Scheduler;

public class QueryEndpoint extends BaseEndpoint {

  private final Scheduler queryResponseScheduler;

  public QueryEndpoint(final ServiceContext ctx, final NetworkAddress hostname,
                       final int port) {
    super(hostname, port, ctx.environment().ioEnvironment().queryEventLoopGroup().get(),
      ctx, ctx.environment().ioConfig().queryCircuitBreakerConfig(), ServiceType.QUERY);
    this.queryResponseScheduler = ctx.environment().scheduler();
  }

  @Override
  protected PipelineInitializer pipelineInitializer() {
    return new QueryPipelineInitializer();
  }

  public class QueryPipelineInitializer implements PipelineInitializer {

    @Override
    public void init(ChannelPipeline pipeline) {
      pipeline.addLast(new LoggingHandler(LogLevel.ERROR));
      pipeline.addLast(new HttpClientCodec());
      pipeline.addLast(new QueryMessageHandler(queryResponseScheduler));
    }
  }

}
