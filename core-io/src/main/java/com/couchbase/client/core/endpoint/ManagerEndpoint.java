package com.couchbase.client.core.endpoint;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.io.NetworkAddress;
import com.couchbase.client.core.io.netty.config.ManagerMessageHandler;
import com.couchbase.client.core.service.ServiceType;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;

public class ManagerEndpoint extends BaseEndpoint {

  private final CoreContext coreContext;

  public ManagerEndpoint(final CoreContext coreContext, final NetworkAddress hostname, final int port) {
    super(hostname, port, coreContext.environment().ioEnvironment().managerEventLoopGroup().get(),
      coreContext, coreContext.environment().ioEnvironment().managerCircuitBreakerConfig(), ServiceType.MANAGER);
    this.coreContext = coreContext;
  }

  @Override
  protected PipelineInitializer pipelineInitializer() {
    return new ManagerPipelineInitializer(coreContext);
  }

  public static class ManagerPipelineInitializer implements PipelineInitializer {

    private final CoreContext coreContext;

    public ManagerPipelineInitializer(CoreContext coreContext) {
      this.coreContext = coreContext;
    }

    @Override
    public void init(ChannelPipeline pipeline) {
      pipeline.addLast(new HttpClientCodec());
      pipeline.addLast(new ManagerMessageHandler(coreContext));
    }
  }
}
