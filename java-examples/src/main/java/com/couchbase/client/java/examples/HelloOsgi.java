/*
 * Copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.java.examples;

import com.couchbase.client.core.deps.io.netty.util.internal.logging.InternalLoggerFactory;
import com.couchbase.client.core.deps.io.netty.util.internal.logging.Slf4JLoggerFactory;
import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

import java.io.File;
import java.time.Duration;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class HelloOsgi implements BundleActivator {
  String username = "Administrator";
  String password = "password";
  String url = "127.0.0.1";
  Cluster cluster;

  public void start(BundleContext ctx) {
    System.out.println("Hello world.");

    try {
      InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
      String configFilename = System.getenv("HOME") + "/log4j.properties";
      File logConfig = new File(configFilename);
      if (logConfig.exists()) {
        System.out.println("log4j.configuration " + configFilename);
        System.setProperty("log4j.configuration",
            "file://" + configFilename);
      } else {
        System.out.println("logging configuration file " + configFilename + " not found, continuing without");
      }
    } catch (java.lang.NoClassDefFoundError e) {
      System.out.println("Slf4JLogging is not going to work, logging will fallback to Console\n");
    }


    System.out.println("Cluster.connect...");
    cluster = Cluster.connect(seedNodes(),
        clusterOptions());
    Bucket bucket = cluster.bucket("travel-sample");
    Collection collection = bucket.defaultCollection();
    bucket.waitUntilReady(Duration.ofSeconds(11));

    String id = UUID.randomUUID()
        .toString();
    collection.upsert(id,
        JsonObject.create()
            .put("name",
                "MyName"));
    GetResult r = bucket.defaultCollection()
        .get(id);
    System.out.println(">>>>>>>>>>>>>> " + r);
  }

  private ClusterOptions options() {
    return ClusterOptions.clusterOptions(username,
        password);
  }

  public void stop(BundleContext bundleContext) {
    System.out.println("Goodbye world.");
    if (cluster != null) {
      cluster.disconnect();
      cluster.environment()
          .shutdown();
    }
  }

  private Set<SeedNode> seedNodes() {
    Set<SeedNode> seedNodes = new HashSet<>();
    SeedNode sn = SeedNode.create(url,
        Optional.of(11210),
        Optional.of(8091));
    seedNodes.add(sn);
    return seedNodes;
  }

  protected ClusterOptions clusterOptions() {
    return ClusterOptions.clusterOptions(authenticator())
        .environment(env -> {
          // Customize environment. For example:
          // env.ioConfig().enableDnsSrv(false);
        });
  }

  protected Authenticator authenticator() {
    return PasswordAuthenticator.create(username,
        password);
  }

}
