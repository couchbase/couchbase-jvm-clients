package com.couchbase.client.core.protostellar.util;

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.CoreProtostellar;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.protostellar.ProtostellarContext;
import com.couchbase.client.core.protostellar.kv.ProtostellarCoreKvOps;
import com.couchbase.client.test.TestClusterConfig;
import com.couchbase.client.test.TestNodeConfig;

import java.time.Duration;
import java.util.Set;

import static com.couchbase.client.core.CoreProtostellar.DEFAULT_PROTOSTELLAR_TLS_PORT;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.core.util.ConnectionStringUtil.asConnectionString;
import static java.util.Objects.requireNonNull;

public class ProtostellarTestEnvironment implements AutoCloseable {
  private final CoreProtostellar core;
  private final ProtostellarCoreKvOps ops;

  private ProtostellarTestEnvironment(CoreProtostellar core, CoreKeyspace defaultKeyspace, ProtostellarCoreKvOps ops) {
    this.core = requireNonNull(core);
    this.defaultKeyspace = defaultKeyspace;
    this.ops = ops;
  }

  private final CoreKeyspace defaultKeyspace;

  public CoreEnvironment env() {
    return core.context().environment();
  }

  public CoreProtostellar core() {
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

    Set<SeedNode> seedNodes = setOf(SeedNode.create(hostname).withProtostellarPort(port));

    CoreProtostellar core = new CoreProtostellar(
      env,
      PasswordAuthenticator.create(config.adminUsername(), config.adminPassword()),
      asConnectionString(seedNodes)
    );

    CoreKeyspace defaultKeyspace = new CoreKeyspace(config.bucketname(), CollectionIdentifier.DEFAULT_SCOPE, CollectionIdentifier.DEFAULT_COLLECTION);

    ProtostellarCoreKvOps ops = new ProtostellarCoreKvOps(core, defaultKeyspace);

    return new ProtostellarTestEnvironment(core, defaultKeyspace, ops);
  }

  @Override
  public void close() {
    core.context().environment().close();
    core.shutdown(Duration.ofSeconds(30));
  }
}
