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

package com.couchbase.client.java.manager.search;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.error.SearchServiceException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.java.json.JacksonTransformers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchIndex {

  private final String name;
  private final String sourceName;

  private String uuid;
  private String type;
  private Map<String, Object> params;
  private String sourceUuid;
  private Map<String, Object> sourceParams;
  private String sourceType;
  private Map<String, Object> planParams;

  public SearchIndex(final String name, final String sourceName) {
    this.name = name;
    this.sourceName = sourceName;
  }

  @JsonCreator
  public SearchIndex(
    @JsonProperty("uuid") String uuid,
    @JsonProperty("name") String name,
    @JsonProperty("type") String type,
    @JsonProperty("params") Map<String, Object> params,
    @JsonProperty("sourceUUID") String sourceUuid,
    @JsonProperty("sourceName") String sourceName,
    @JsonProperty("sourceParams") Map<String, Object> sourceParams,
    @JsonProperty("sourceType") String sourceType,
    @JsonProperty("planParams") Map<String, Object> planParams) {
    this.uuid = uuid;
    this.name = name;
    this.sourceName = sourceName;
    this.type = type;
    this.params = params;
    this.sourceUuid = sourceUuid;
    this.sourceParams = sourceParams;
    this.sourceType = sourceType;
    this.planParams = planParams;
  }

  public String name() {
    return name;
  }

  public String uuid() {
    return uuid;
  }

  public void uuid(String uuid) {
    this.uuid = uuid;
  }

  public String sourceName() {
    return sourceName;
  }

  public String type() {
    return type;
  }

  public Map<String, Object> params() {
    return params;
  }

  public void params(Map<String, Object> params) {
    this.params = params;
  }

  public String sourceUuid() {
    return sourceUuid;
  }

  public void sourceUuid(String sourceUuid) {
    this.sourceUuid = sourceUuid;
  }

  public Map<String, Object> sourceParams() {
    return sourceParams;
  }

  public void sourceParams(Map<String, Object> sourceParams) {
    this.sourceParams = sourceParams;
  }

  public String sourceType() {
    return sourceType;
  }

  public void sourceType(String sourceType) {
    this.sourceType = sourceType;
  }

  public Map<String, Object> planParams() {
    return planParams;
  }

  public void planParams(Map<String, Object> planParams) {
    this.planParams = planParams;
  }

  public String toJson() {
    Map<String, Object> output = new HashMap<>();
    output.put("name", name);
    output.put("sourceName", sourceName);
    output.put("type", type == null ? "fulltext-index" : type);
    output.put("sourceType", sourceType == null ? "couchbase" : sourceType);
    return Mapper.encodeAsString(output);
  }

  @Override
  public String toString() {
    return "SearchIndex{" +
      "uuid='" + uuid + '\'' +
      ", name='" + name + '\'' +
      ", sourceName='" + sourceName + '\'' +
      ", type='" + type + '\'' +
      ", params=" + params +
      ", sourceUuid='" + sourceUuid + '\'' +
      ", sourceParams=" + sourceParams +
      ", sourceType='" + sourceType + '\'' +
      ", planParams=" + planParams +
      '}';
  }
}
