/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.java.manager.eventing;

import com.couchbase.client.core.annotation.Stability;

import java.util.ArrayList;
import java.util.List;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

/**
 * An immutable representation of the {@link EventingFunction} stored or to be stored on the server.
 */
@Stability.Uncommitted
public class EventingFunction {

  private final String name;
  private final String code;
  private final EventingFunctionKeyspace sourceKeyspace;
  private final EventingFunctionKeyspace metadataKeyspace;
  private final EventingFunctionSettings settings;
  private final boolean enforceSchema;
  private final String version;
  private final int handlerUuid;
  private final String functionInstanceId;
  private final List<EventingFunctionBucketBinding> bucketBindings;
  private final List<EventingFunctionUrlBinding> urlBindings;
  private final List<EventingFunctionConstantBinding> constantBindings;

  /**
   * Creates a new {@link EventingFunction} with the minimal required properties.
   *
   * @param name the name of the function.
   * @param code the code body of the function.
   * @param sourceKeyspace the keyspace from where the source data is coming from.
   * @param metadataKeyspace the keyspace where the function metadata is stored.
   * @return the created {@link EventingFunction}.
   */
  public static EventingFunction create(String name, String code, EventingFunctionKeyspace sourceKeyspace,
                                EventingFunctionKeyspace metadataKeyspace) {
    return builder(name, code, sourceKeyspace, metadataKeyspace).build();
  }

  /**
   * Creates a {@link Builder} that can be used to further customize the {@link EventingFunction} beyond the defaults.
   *
   * @param name the name of the function.
   * @param code the code body of the function.
   * @param sourceKeyspace the keyspace from where the source data is coming from.
   * @param metadataKeyspace the keyspace where the function metadata is stored.
   * @return the builder to customize.
   */
  public static Builder builder(String name, String code, EventingFunctionKeyspace sourceKeyspace,
                                EventingFunctionKeyspace metadataKeyspace) {
    return new Builder(name, code, sourceKeyspace, metadataKeyspace);
  }

  private EventingFunction(Builder builder) {
    this.settings = builder.settings;
    this.name = builder.name;
    this.code = builder.code;
    this.sourceKeyspace = builder.sourceKeyspace;
    this.metadataKeyspace = builder.metadataKeyspace;
    this.bucketBindings = builder.bucketBindings;
    this.urlBindings = builder.urlBindings;
    this.constantBindings = builder.constantBindings;
    this.enforceSchema = builder.enforceSchema;
    this.handlerUuid = builder.handlerUuid;
    this.functionInstanceId = builder.functionInstanceId;
    this.version = builder.version;
  }

  /**
   * The name of the function.
   */
  public String name() {
    return name;
  }

  /**
   * The actual javascript source code of the function.
   */
  public String code() {
    return code;
  }

  /**
   * The source keyspace where the actual data is accessed from.
   */
  public EventingFunctionKeyspace sourceKeyspace() {
    return sourceKeyspace;
  }

  /**
   * The keyspace where eventing stores the metadata for the function.
   */
  public EventingFunctionKeyspace metadataKeyspace() {
    return metadataKeyspace;
  }

  /**
   * The version of the function.
   */
  public String version() {
    return version;
  }

  /**
   * True if the schema should be enforced.
   */
  public boolean enforceSchema() {
    return enforceSchema;
  }

  /**
   * The function UUID, provided by the server.
   */
  public long handlerUuid() {
    return handlerUuid;
  }

  /**
   * The function instance ID, provided by the server.
   */
  public String functionInstanceId() {
    return functionInstanceId;
  }

  /**
   * The custom function settings applied.
   */
  public EventingFunctionSettings settings() {
    return settings;
  }

  /**
   * The bucket bindings for the function.
   */
  public List<EventingFunctionBucketBinding> bucketBindings() {
    return bucketBindings;
  }

  /**
   * The URL bindings for the function.
   */
  public List<EventingFunctionUrlBinding> urlBindings() {
    return urlBindings;
  }

  /**
   * The constant bindings for the function.
   */
  public List<EventingFunctionConstantBinding> constantBindings() {
    return constantBindings;
  }

  /**
   * This builder allows to customize the properties of the eventing function.
   */
  public static class Builder {

    private final String name;
    private final String code;
    private final EventingFunctionKeyspace sourceKeyspace;
    private final EventingFunctionKeyspace metadataKeyspace;

    private EventingFunctionSettings settings = EventingFunctionSettings.create();
    private boolean enforceSchema = false;
    private List<EventingFunctionBucketBinding> bucketBindings = new ArrayList<>();
    private List<EventingFunctionUrlBinding> urlBindings = new ArrayList<>();
    private List<EventingFunctionConstantBinding> constantBindings = new ArrayList<>();

    private String version;
    private int handlerUuid;
    private String functionInstanceId;

    private Builder(String name, String code, EventingFunctionKeyspace sourceKeyspace,
                    EventingFunctionKeyspace metadataKeyspace) {
      this.name = notNullOrEmpty(name, "Name");
      this.code = notNullOrEmpty(code, "Code");
      this.sourceKeyspace = notNull(sourceKeyspace, "SourceKeyspace");
      this.metadataKeyspace = notNull(metadataKeyspace, "MetadataKeyspace");
    }

    /**
     * Sets the URL bindings for the function.
     *
     * @param urlBindings the URL bindings for the function.
     * @return the {@link Builder} for chaining purposes.
     */
    public Builder urlBindings(List<EventingFunctionUrlBinding> urlBindings) {
      this.urlBindings = new ArrayList<>(notNull(urlBindings, "EventingFunctionUrlBinding"));
      return this;
    }

    /**
     * Sets the bucket bindings for the function.
     *
     * @param bucketBindings the bucket bindings for the function.
     * @return the {@link Builder} for chaining purposes.
     */
    public Builder bucketBindings(List<EventingFunctionBucketBinding> bucketBindings) {
      this.bucketBindings = new ArrayList<>(notNull(bucketBindings, "EventingFunctionBucketBinding"));
      return this;
    }

    /**
     * Sets the constant bindings for the function.
     *
     * @param constantBindings the constant bindings for the function.
     * @return the {@link Builder} for chaining purposes.
     */
    public Builder constantBindings(List<EventingFunctionConstantBinding> constantBindings) {
      this.constantBindings = new ArrayList<>(notNull(constantBindings, "EventingFunctionConstantBinding"));
      return this;
    }

    /**
     * Set to true if the schema schould be enforced.
     *
     * @param enforceSchema true if it should be enforced.
     * @return the {@link Builder} for chaining purposes.
     */
    public Builder enforceSchema(boolean enforceSchema) {
      this.enforceSchema = enforceSchema;
      return this;
    }

    /**
     * Sets various function settings to customize the runtime behavior.
     *
     * @param settings the settings to apply to the function.
     * @return the {@link Builder} for chaining purposes.
     */
    public Builder settings(EventingFunctionSettings settings) {
      this.settings = settings;
      return this;
    }

    /**
     * (internal) Sets the version of the function, set when loading from the server.
     *
     * @param version the version provided from the server.
     * @return the {@link Builder} for chaining purposes.
     */
    Builder version(String version) {
      this.version = version;
      return this;
    }

    /**
     * (internal) Sets the handler UUID.
     *
     * @param handlerUuid the handler UUID received from the server.
     * @return the {@link Builder} for chaining purposes.
     */
    Builder handlerUuid(int handlerUuid) {
      this.handlerUuid = handlerUuid;
      return this;
    }

    /**
     * (internal) Sets the function instance id.
     *
     * @param functionInstanceId the function instance ID received from the server.
     * @return the {@link Builder} for chaining purposes.
     */
    Builder functionInstanceId(String functionInstanceId) {
      this.functionInstanceId = functionInstanceId;
      return this;
    }

    /**
     * Builds the immutable {@link EventingFunction}.
     */
    public EventingFunction build() {
      return new EventingFunction(this);
    }

  }

  @Override
  public String toString() {
    return "EventingFunction{" +
      "name='" + name + '\'' +
      ", code='" + code + '\'' +
      ", sourceKeyspace=" + sourceKeyspace +
      ", metadataKeyspace=" + metadataKeyspace +
      ", settings=" + settings +
      ", version='" + version + '\'' +
      ", enforceSchema=" + enforceSchema +
      ", handlerUuid=" + handlerUuid +
      ", functionInstanceId='" + functionInstanceId + '\'' +
      ", bucketBindings=" + bucketBindings +
      ", urlBindings=" + urlBindings +
      ", constantBindings=" + constantBindings +
      '}';
  }
}
