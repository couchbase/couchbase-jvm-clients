package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.config.ProposedGlobalConfigContext;
import com.couchbase.client.core.io.CollectionMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Helper class for integration tests to measure the proposed bucket configurations.
 */
public class ProposedBucketConfigInspectingProvider implements ConfigurationProvider {

  private final ConfigurationProvider delegate;
  private final List<ProposedBucketConfigContext> configs = Collections.synchronizedList(new ArrayList<>());
  private final List<Long> timings = Collections.synchronizedList(new ArrayList<>());

  ProposedBucketConfigInspectingProvider(final ConfigurationProvider delegate) {
    this.delegate = delegate;
  }

  @Override
  public void proposeBucketConfig(final ProposedBucketConfigContext ctx) {
    timings.add(System.nanoTime());
    configs.add(ctx);
    delegate.proposeBucketConfig(ctx);
  }

  List<ProposedBucketConfigContext> proposedConfigs() {
    return configs;
  }

  List<Long> proposedTimings() {
    return timings;
  }

  @Override
  public Flux<ClusterConfig> configs() {
    return delegate.configs();
  }

  @Override
  public ClusterConfig config() {
    return delegate.config();
  }

  @Override
  public Mono<Void> openBucket(String name) {
    return delegate.openBucket(name);
  }

  @Override
  public Mono<Void> closeBucket(String name) {
    return delegate.closeBucket(name);
  }

  @Override
  public Mono<Void> shutdown() {
    return delegate.shutdown();
  }

  @Override
  public void proposeGlobalConfig(ProposedGlobalConfigContext ctx) {
    delegate.proposeGlobalConfig(ctx);
  }

  @Override
  public Mono<Void> loadAndRefreshGlobalConfig() {
    return delegate.loadAndRefreshGlobalConfig();
  }

  @Override
  public CollectionMap collectionMap() {
    return delegate.collectionMap();
  }

  @Override
  public Mono<Void> refreshCollectionMap(String bucket, boolean force) {
    return delegate.refreshCollectionMap(bucket, force);
  }

}
