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

import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.ConnectionStringPropertyLoader;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.java.codec.DefaultTranscoder;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.Serializer;
import com.couchbase.client.java.codec.Transcoder;

import static com.couchbase.client.core.util.Validators.notNull;
import com.couchbase.client.core.env.PasswordAuthenticator;

public class ClusterEnvironment extends CoreEnvironment {

  private final Serializer jsonSerializer;
  private final Transcoder transcoder;

  private ClusterEnvironment(Builder builder) {
    super(builder);
    this.jsonSerializer = builder.jsonSerializer == null ? JsonSerializer.create() : builder.jsonSerializer;
    this.transcoder = builder.transcoder == null ? DefaultTranscoder.create(jsonSerializer) : builder.transcoder;
  }

  @Override
  protected String defaultAgentTitle() {
    return "java";
  }

  public static ClusterEnvironment create(final String username, final String password) {
    return builder(username, password).build();
  }

  public static ClusterEnvironment create(final Authenticator authenticator) {
    return builder(authenticator).build();
  }

  public static ClusterEnvironment create(final String connectionString, String username, String password) {
    return builder(connectionString, username, password).build();
  }

  public static ClusterEnvironment create(final String connectionString, Authenticator authenticator) {
    return builder(connectionString, authenticator).build();
  }

  public static ClusterEnvironment.Builder builder(final String username, final String password) {
    return builder(PasswordAuthenticator.create(username, password));
  }

  public static ClusterEnvironment.Builder builder(final Authenticator authenticator) {
    return new ClusterEnvironment.Builder(authenticator);
  }

  public static ClusterEnvironment.Builder builder(final String connectionString, final String username, final String password) {
    return builder(connectionString, PasswordAuthenticator.create(username, password));
  }

  public static ClusterEnvironment.Builder builder(final String connectionString, final Authenticator authenticator) {
    return builder(authenticator).load(new ConnectionStringPropertyLoader(connectionString));
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
  public Serializer jsonSerializer() {
    return jsonSerializer;
  }

  public static class Builder extends CoreEnvironment.Builder<Builder> {

    private Serializer jsonSerializer;
    private Transcoder transcoder;

    Builder(Authenticator authenticator) {
      super(authenticator);
    }

    public Builder load(final ClusterPropertyLoader loader) {
      loader.load(this);
      return this;
    }

    /**
     * Allows to override the default serializer going to be used for all JSON values.
     *
     * @param jsonSerializer the serializer used for all JSON values.
     * @return this builder for chaining purposes.
     */
    public Builder jsonSerializer(final Serializer jsonSerializer) {
      notNull(jsonSerializer, "Json Serializer");
      this.jsonSerializer = jsonSerializer;
      return this;
    }

    /**
     * Allows to override the default transcoder going to be used for all KV operations.
     *
     * @param transcoder the transcoder that should be used by default.
     * @return this builder for chaining purposes.
     */
    public Builder transcoder(final Transcoder transcoder) {
      notNull(transcoder, "Transcoder");
      this.transcoder = transcoder;
      return this;
    }

    public ClusterEnvironment build() {
      return new ClusterEnvironment(this);
    }
  }
}
