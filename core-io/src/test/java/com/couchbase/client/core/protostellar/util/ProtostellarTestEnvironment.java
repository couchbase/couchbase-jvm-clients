package com.couchbase.client.core.protostellar.util;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.protostellar.kv.ProtostellarCoreKvOps;
import com.couchbase.client.core.util.CbCollections;
import com.couchbase.client.test.TestClusterConfig;
import com.couchbase.client.test.TestNodeConfig;

import static com.couchbase.client.core.CoreProtostellar.DEFAULT_PROTOSTELLAR_TLS_PORT;

public class ProtostellarTestEnvironment implements AutoCloseable {
  private final CoreEnvironment env;
  private final Core core;
  private final ProtostellarCoreKvOps ops;

  public ProtostellarTestEnvironment(CoreEnvironment env, Core core, CoreKeyspace defaultKeyspace, ProtostellarCoreKvOps ops) {
    this.env = env;
    this.core = core;
    this.defaultKeyspace = defaultKeyspace;
    this.ops = ops;
  }

  private final CoreKeyspace defaultKeyspace;

  public CoreEnvironment env() {
    return env;
  }

  public Core core() {
    return core;
  }

  public CoreKeyspace defaultKeyspace() {
    return defaultKeyspace;
  }

  public ProtostellarCoreKvOps ops() {
    return ops;
  }

  public static ProtostellarTestEnvironment create(TestClusterConfig config) {
    CoreEnvironment env = new CoreTestEnvironment(CoreEnvironment.builder());
    TestNodeConfig node = config.nodes().get(0);
    String hostname = node.hostname();
    int port = node.protostellarPort().orElse(DEFAULT_PROTOSTELLAR_TLS_PORT);

    Core core = new CoreTest(env,
      PasswordAuthenticator.create(config.adminUsername(), config.adminPassword()),
      CbCollections.setOf(SeedNode.create(hostname).withProtostellarPort(port)),
      String.format("protostellar://%s:%d", hostname, port));

    CoreKeyspace defaultKeyspace = new CoreKeyspace(config.bucketname(), CollectionIdentifier.DEFAULT_SCOPE, CollectionIdentifier.DEFAULT_COLLECTION);

    ProtostellarCoreKvOps ops = new ProtostellarCoreKvOps(core, defaultKeyspace);

    return new ProtostellarTestEnvironment(env, core, defaultKeyspace, ops);
  }

  @Override
  public void close() {
    env.close();
    core.close();
  }
}
