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

package com.couchbase.client.java.env;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.annotation.UsedBy;
import com.couchbase.client.core.encryption.CryptoManager;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.VersionAndGitHash;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.codec.DefaultJsonSerializer;
import com.couchbase.client.java.codec.JacksonJsonSerializer;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.JsonTranscoder;
import com.couchbase.client.java.codec.JsonValueSerializerWrapper;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.json.JsonObjectCrypto;
import com.couchbase.client.java.transactions.config.TransactionsConfig;

import java.util.Optional;
import java.util.function.Consumer;

import static com.couchbase.client.core.annotation.UsedBy.Project.QUARKUS_COUCHBASE;
import static com.couchbase.client.core.util.CbObjects.defaultIfNull;
import static com.couchbase.client.core.util.Validators.notNull;

/**
 * The Environment is the main place in the SDK where configuration and state lives (i.e. I/O pools).
 * <p>
 * If not created explicitly by the user, the SDK will create its own one internally that can also be accessed
 * by the user. If this is the case it does not need to be shut down manually. If the environment is passed in
 * to the {@link ClusterOptions} though the user is responsible for shutting it down after
 * the cluster is disconnected!
 * <p>
 * Since the {@link ClusterEnvironment} extends the {@link CoreEnvironment}, most options can be found on the parent
 * class for documentation purposes.
 */
public class ClusterEnvironment extends CoreEnvironment {
  private static final VersionAndGitHash clientVersion = VersionAndGitHash.from(Cluster.class);

  private final JsonSerializer jsonSerializer;
  private final Transcoder transcoder;
  private final Optional<CryptoManager> cryptoManager;

  private ClusterEnvironment(Builder builder) {
    super(builder);
    this.jsonSerializer = builder.jsonSerializer != null
        ? new JsonValueSerializerWrapper(builder.jsonSerializer)
        : newDefaultSerializer(builder.cryptoManager);
    this.transcoder = defaultIfNull(builder.transcoder, () -> JsonTranscoder.create(jsonSerializer));
    this.cryptoManager = Optional.ofNullable(builder.cryptoManager);
  }

  /**
   * Creates the default JSON serializer, checking if an external jackson is present or not.
   * <p>
   * Be very careful not to reference any classes from the optional Jackson library otherwise users will get
   * NoClassDefFoundError when Jackson is absent.
   */
  @UsedBy(QUARKUS_COUCHBASE)
  private JsonSerializer newDefaultSerializer(CryptoManager cryptoManager) {
    return nonShadowedJacksonPresent()
        ? JacksonJsonSerializer.create(cryptoManager)
        : DefaultJsonSerializer.create(cryptoManager);
  }

  /**
   * Returns true if a non-shadowed Jackson library is present, otherwise false.
   */
  private boolean nonShadowedJacksonPresent() {
    try {
      JacksonJsonSerializer.preflightCheck();
      return true;
    } catch (Throwable t) {
      return false;
    }
  }

  @Override
  protected String defaultAgentTitle() {
    return "java";
  }

  @Override
  protected VersionAndGitHash clientVersionAndGitHash() {
    return clientVersion;
  }

  /**
   * Creates a new {@link ClusterEnvironment} with default settings.
   *
   * @return a new environment with default settings.
   */
  public static ClusterEnvironment create() {
    return builder().build();
  }

  /**
   * Creates a {@link Builder} to customize the properties of the environment.
   *
   * @return the {@link Builder} to customize.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the default transcoder used for all operations if not overridden on a per-operation basis.
   */
  public Transcoder transcoder() {
    return transcoder;
  }

  /**
   * Returns the default serializer used to serialize and deserialize JSON values.
   */
  public JsonSerializer jsonSerializer() {
    return jsonSerializer;
  }

  /**
   * Returns the low-level cryptography manager for Field-Level Encryption if one has been configured.
   * <p>
   * Useful for implementing encryption with external JSON libraries.
   * <p>
   * See {@link JsonObjectCrypto} for a high level alternative that's compatible
   * with Couchbase {@code JsonObject}s.
   * <p>
   * Note: Use of the Field-Level Encryption functionality is
   * subject to the <a href="https://www.couchbase.com/ESLA01162020">
   * Couchbase Inc. Enterprise Subscription License Agreement v7</a>
   *
   * @see Builder#cryptoManager(CryptoManager)
   */
  public Optional<CryptoManager> cryptoManager() {
    return cryptoManager;
  }

  public static class Builder extends CoreEnvironment.Builder<Builder> {

    private JsonSerializer jsonSerializer;
    private Transcoder transcoder;
    private CryptoManager cryptoManager;
    private TransactionsConfig.Builder transactionsConfigBuilder = TransactionsConfig.builder();

    Builder() {
      super();
    }

    /**
     * Immediately loads the properties from the given loader into the environment.
     *
     * @param loader the loader to load the properties from.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder load(final ClusterPropertyLoader loader) {
      notNull(loader, "ClusterPropertyLoader");
      loader.load(this);
      return this;
    }

    /**
     * Sets the default serializer for converting between JSON and Java objects.
     * <p>
     * Providing your own serializer gives you complete control over the conversion.
     * <p>
     * If this method is not called, the client's behavior depends on whether Jackson is in
     * the class path. If Jackson is present, an instance of {@link JacksonJsonSerializer}
     * will be used. Otherwise the client will fall back to {@link DefaultJsonSerializer}.
     *
     * @param jsonSerializer the serializer used for all JSON values.
     * @return this {@link Builder} for chaining purposes.
     * @see JacksonJsonSerializer
     */
    public Builder jsonSerializer(final JsonSerializer jsonSerializer) {
      this.jsonSerializer = notNull(jsonSerializer, "JsonSerializer");
      return this;
    }

    /**
     * Allows to override the default transcoder going to be used for all KV operations.
     *
     * @param transcoder the transcoder that should be used by default.
     * @return this {@link Builder} for chaining purposes.
     */
    public Builder transcoder(final Transcoder transcoder) {
      this.transcoder = notNull(transcoder, "Transcoder");
      return this;
    }

    /**
     * Sets the cryptography manager for Field-Level Encryption
     * (reading and writing encrypted document fields).
     * <p>
     * Note: Use of the Field-Level Encryption functionality is
     * subject to the <a href="https://www.couchbase.com/ESLA01162020">
     * Couchbase Inc. Enterprise Subscription License Agreement v7</a>
     *
     * @param cryptoManager (nullable) the manager to use, or null to disable encryption support.
     * @return this builder for chaining purposes.
     */
    public Builder cryptoManager(CryptoManager cryptoManager) {
      this.cryptoManager = cryptoManager;
      return this;
    }

    /**
     * Sets the configuration for all transactions.
     *
     * @param transactionsConfig the transactions configuration.
     * @return this builder for chaining purposes.
     * @deprecated This method clobbers any previously configured values. Please use {@link #transactionsConfig(Consumer)} instead.
     */
    @Deprecated
    @Stability.Uncommitted
    public Builder transactionsConfig(final TransactionsConfig.Builder transactionsConfig) {
      notNull(transactionsConfig, "transactionsConfig");
      this.transactionsConfigBuilder = transactionsConfig;
      return this;
    }

    /**
     * Passes the {@link TransactionsConfig.Builder} to the provided consumer.
     * <p>
     * Allows customizing the default options for all transactions.
     *
     * @param builderConsumer a callback that configures options.
     * @return this builder for chaining purposes.
     */
    @Stability.Uncommitted
    public Builder transactionsConfig(Consumer<TransactionsConfig.Builder> builderConsumer) {
      builderConsumer.accept(this.transactionsConfigBuilder);
      return this;
    }

    /**
     * Turns this builder into a real {@link ClusterEnvironment}.
     *
     * @return the created cluster environment.
     */
    public ClusterEnvironment build() {
      this.transactionsConfig = transactionsConfigBuilder.build();
      return new ClusterEnvironment(this);
    }

  }
}
