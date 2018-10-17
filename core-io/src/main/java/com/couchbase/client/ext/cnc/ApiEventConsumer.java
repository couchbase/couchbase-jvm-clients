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

package com.couchbase.client.ext.cnc;

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.events.io.ErrorMapLoadingFailedEvent;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.ext.cnc.api.State;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.CharsetUtil;

import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;

import static graphql.schema.idl.RuntimeWiring.newRuntimeWiring;

/**
 * Consumes SDK events and exposes its aggregated status over a web API.
 *
 * @since 2.0.0
 */
public class ApiEventConsumer implements Consumer<Event> {

  String schema = "type Warning {" +
    "  name: String" +
    "  description: String" +
    "}" +
    "" +
    "" +
    "type Query {" +
    "  warnings: [Warning]" +
    "}";

  private final State state;
  private final GraphQL gq;

  public static Builder builder() {
    return new Builder();
  }

  public static ApiEventConsumer create() {
    return builder().build();
  }

  private ApiEventConsumer(Builder builder) {
    this.state = new State();

    state.accept(new ErrorMapLoadingFailedEvent(null, Duration.ZERO, (short) 1));

    SchemaParser schemaParser = new SchemaParser();
    TypeDefinitionRegistry typeDefinitionRegistry = schemaParser.parse(schema);

    RuntimeWiring runtimeWiring = newRuntimeWiring()
      .type("Query", b -> b.dataFetcher("warnings", state))
      .build();

    SchemaGenerator schemaGenerator = new SchemaGenerator();
    GraphQLSchema graphQLSchema = schemaGenerator.makeExecutableSchema(
      typeDefinitionRegistry,
      runtimeWiring
    );

    gq = GraphQL.newGraphQL(graphQLSchema).build();
  }

  public void start() {
    EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    EventLoopGroup workerGroup = new NioEventLoopGroup();

      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline()
              .addLast(new HttpServerCodec())
              .addLast(new HttpObjectAggregator(Integer.MAX_VALUE))
              .addLast(new SimpleChannelInboundHandler<FullHttpRequest>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
                  if (msg.uri().startsWith("/graphql") && msg.method().equals(HttpMethod.GET)) {
                    QueryStringDecoder decoder = new QueryStringDecoder(msg.uri());
                    String q = decoder.parameters().get("query").get(0);
                    ExecutionResult executionResult = gq.execute(q);
                    Map<String, Object> result = executionResult.toSpecification();
                    ByteBuf content = Unpooled.copiedBuffer(Mapper.encodeAsString(result), CharsetUtil.UTF_8);
                    FullHttpResponse response = new DefaultFullHttpResponse(
                      HttpVersion.HTTP_1_1,
                      HttpResponseStatus.OK,
                      content
                    );
                    response.headers().set("Content-Type", "application/json");
                    response.headers().setInt("Content-Length", response.content().readableBytes());
                    ctx.writeAndFlush(response);
                  }
                }
              });
          }
        });

    try {
      Channel ch = b.bind(8080).sync().channel();
      System.err.println("Open your web browser and navigate to http://127.0.0.1:" + 8080 + '/');
      ch.closeFuture().sync();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void stop() {
    // stop the netty server

  }

  @Override
  public void accept(final Event event) {
    state.accept(event);
  }

  public static class Builder {

    public ApiEventConsumer build() {
      return new ApiEventConsumer(this);
    }
  }

  public static void main(String... args) {
    ApiEventConsumer c = ApiEventConsumer.create();
    c.start();
  }

}
