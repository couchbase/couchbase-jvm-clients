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

import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.java.codec.DefaultJsonSerializer;
import com.couchbase.client.java.codec.JacksonJsonSerializer;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.JsonTranscoder;
import com.couchbase.client.java.codec.Transcoder;

import static com.couchbase.client.core.util.CbObjects.defaultIfNull;
import static com.couchbase.client.core.util.Validators.notNull;

public class ClusterEnvironment extends CoreEnvironment {

  private final JsonSerializer jsonSerializer;
  private final Transcoder transcoder;

  private ClusterEnvironment(Builder builder) {
    super(builder);
    this.jsonSerializer = defaultIfNull(builder.jsonSerializer, () -> newDefaultSerializer());
    this.transcoder = defaultIfNull(builder.transcoder, () -> JsonTranscoder.create(jsonSerializer));
  }

  private JsonSerializer newDefaultSerializer() {
    // Be very careful not to reference any classes from the optional Jackson library
    // otherwise users will get NoClassDefFoundError when Jackson is absent.
    return jacksonIsPresent() ? JacksonJsonSerializer.create() : DefaultJsonSerializer.create();
  }

  private boolean jacksonIsPresent() {
    try {
      Class.forName("com.fasterxml.jackson.databind.ObjectMapper", false, getClass().getClassLoader());
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  @Override
  protected String defaultAgentTitle() {
    return "java";
  }

  public static ClusterEnvironment create() {
    return builder().build();
  }

  public static ClusterEnvironment.Builder builder() {
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

  public static class Builder extends CoreEnvironment.Builder<Builder> {

    private JsonSerializer jsonSerializer;
    private Transcoder transcoder;

    Builder() {
      super();
    }

    public Builder load(final ClusterPropertyLoader loader) {
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
     * @return this builder for chaining purposes.
     * @see JacksonJsonSerializer
     */
    public Builder jsonSerializer(final JsonSerializer jsonSerializer) {
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
