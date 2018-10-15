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

package tmp;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.cnc.DefaultEventBus;
import com.couchbase.client.core.env.CoreEnvironment;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This will go away, it is just to demo the kv connect steps in a
 * holistic picture until it is complete.
 */
public class ConnectSample {

  public static void main(String... args) throws Exception {
    DefaultEventBus eventBus = DefaultEventBus.create();
    CoreEnvironment coreEnvironment = mock(CoreEnvironment.class);
    when(coreEnvironment.eventBus()).thenReturn(eventBus);
    when(coreEnvironment.userAgent()).thenReturn("core-io");

    final CoreContext ctx = new CoreContext(1234, coreEnvironment);

    ChannelFuture connect = new Bootstrap()
      .handler(new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel ch)  {
          ch.pipeline().addLast(new FeatureNegotiatingHand)
        }
      })
      .connect("127.0.0.1", 11210);

    connect.awaitUninterruptibly();


    Thread.sleep(100000);
  }
}
