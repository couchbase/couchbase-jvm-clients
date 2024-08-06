/*
 * Copyright (c) 2024 Couchbase, Inc.
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

package com.couchbase.columnar;

import com.couchbase.client.core.logging.LogRedaction;
import com.couchbase.client.core.logging.RedactionLevel;
import com.couchbase.columnar.rpc.IntraServiceContext;
import com.couchbase.columnar.rpc.JavaColumnarCrossService;
import com.couchbase.columnar.rpc.JavaColumnarService;
import com.couchbase.columnar.fit.core.util.VersionUtil;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class JavaColumnarPerformer {
  private static final Logger logger = LoggerFactory.getLogger(JavaColumnarPerformer.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    System.out.println(VersionUtil.introspectSDKVersionJava());
    int port = 8060;

    // Force that log redaction has been enabled
    LogRedaction.setRedactionLevel(RedactionLevel.PARTIAL);

    var sharedContext = new IntraServiceContext();

    Server server = ServerBuilder.forPort(port)
      .addService(new JavaColumnarService(sharedContext))
      .addService(new JavaColumnarCrossService(sharedContext))
      .build();

    server.start();
    logger.info("Server Started at {}", server.getPort());
    server.awaitTermination();
  }

}
