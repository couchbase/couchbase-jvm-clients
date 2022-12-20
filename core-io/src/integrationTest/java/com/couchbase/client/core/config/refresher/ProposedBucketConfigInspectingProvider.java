package com.couchbase.client.core.config.refresher;

import com.couchbase.client.core.config.ClusterConfig;
import com.couchbase.client.core.config.ConfigRefreshFailure;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.config.ProposedBucketConfigContext;
import com.couchbase.client.core.config.ProposedGlobalConfigContext;
import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.CollectionMap;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;

/**
 * Helper class for integration tests to measure the proposed bucket configurations.
 */
public class ProposedBucketConfigInspectingProvider implements ConfigurationProvider {

  public static class ProposedConfigAndTimestamp {
    private final ProposedBucketConfigContext config;
    private final long nanoTime;

    public ProposedConfigAndTimestamp(ProposedBucketConfigContext config, long nanoTime) {
      this.config = requireNonNull(config);
      this.nanoTime = nanoTime;
    }

    public ProposedBucketConfigContext proposedConfig() {
      return config;
    }

    public long nanosSince(ProposedConfigAndTimestamp other) {
      return nanoTime - other.nanoTime;
    }
  }

  private final ConfigurationProvider delegate;
  private final List<ProposedConfigAndTimestamp> configs = new CopyOnWriteArrayList<>();

  ProposedBucketConfigInspectingProvider(final ConfigurationProvider delegate) {
    this.delegate = delegate;
  }

  @Override
  public void proposeBucketConfig(final ProposedBucketConfigContext ctx) {
    configs.add(new ProposedConfigAndTimestamp(ctx, System.nanoTime()));
    delegate.proposeBucketConfig(ctx);
  }

  List<ProposedConfigAndTimestamp> proposedConfigs() {
    return configs;
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
  public Flux<Set<SeedNode>> seedNodes() {
    return delegate.seedNodes();
  }

  @Override
  public Mono<Void> openBucket(String name) {
    return delegate.openBucket(name);
  }

  @Override
  public Mono<Void> closeBucket(String name, boolean pushConfig) {
    return delegate.closeBucket(name, pushConfig);
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
  public boolean bucketConfigLoadInProgress() {
    return false;
  }

  @Override
  public CollectionMap collectionMap() {
    return delegate.collectionMap();
  }

  @Override
  public void refreshCollectionId(CollectionIdentifier identifier) {
    delegate.refreshCollectionId(identifier);
  }

  @Override
  public boolean globalConfigLoadInProgress() {
    return false;
  }

  @Override
  public boolean collectionRefreshInProgress() {
    return delegate.collectionRefreshInProgress();
  }

  @Override
  public boolean collectionRefreshInProgress(CollectionIdentifier identifier) {
    return delegate.collectionRefreshInProgress(identifier);
  }

  @Override
  public void signalConfigRefreshFailed(ConfigRefreshFailure failure) {

  }
}
