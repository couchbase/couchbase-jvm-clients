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

  public static EventingFunction create(String name, String code, EventingFunctionKeyspace sourceKeyspace,
                                EventingFunctionKeyspace metadataKeyspace) {
    return builder(name, code, sourceKeyspace, metadataKeyspace).build();
  }

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

  public String name() {
    return name;
  }

  public String code() {
    return code;
  }

  public EventingFunctionKeyspace sourceKeyspace() {
    return sourceKeyspace;
  }

  public EventingFunctionKeyspace metadataKeyspace() {
    return metadataKeyspace;
  }

  public String version() {
    return version;
  }

  public boolean enforceSchema() {
    return enforceSchema;
  }

  public long handlerUuid() {
    return handlerUuid;
  }

  public String functionInstanceId() {
    return functionInstanceId;
  }

  public EventingFunctionSettings settings() {
    return settings;
  }

  public List<EventingFunctionBucketBinding> bucketBindings() {
    return bucketBindings;
  }

  public List<EventingFunctionUrlBinding> urlBindings() {
    return urlBindings;
  }

  public List<EventingFunctionConstantBinding> constantBindings() {
    return constantBindings;
  }

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

    public Builder urlBindings(List<EventingFunctionUrlBinding> urlBindings) {
      this.urlBindings = new ArrayList<>(notNull(urlBindings, "EventingFunctionUrlBinding"));
      return this;
    }

    public Builder bucketBindings(List<EventingFunctionBucketBinding> bucketBindings) {
      this.bucketBindings = new ArrayList<>(notNull(bucketBindings, "EventingFunctionBucketBinding"));
      return this;
    }

    public Builder constantBindings(List<EventingFunctionConstantBinding> constantBindings) {
      this.constantBindings = new ArrayList<>(notNull(constantBindings, "EventingFunctionConstantBinding"));
      return this;
    }

    public Builder enforceSchema(boolean enforceSchema) {
      this.enforceSchema = enforceSchema;
      return this;
    }

    public Builder settings(EventingFunctionSettings settings) {
      this.settings = settings;
      return this;
    }

    Builder version(String version) {
      this.version = version;
      return this;
    }

    Builder handlerUuid(int handlerUuid) {
      this.handlerUuid = handlerUuid;
      return this;
    }

    Builder functionInstanceId(String functionInstanceId) {
      this.functionInstanceId = functionInstanceId;
      return this;
    }

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
