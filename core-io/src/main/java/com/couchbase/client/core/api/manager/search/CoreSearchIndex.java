/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.core.api.manager.search;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ArrayNode;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.json.Mapper;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;


@Stability.Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public class CoreSearchIndex {

  private final String name;
  private final String sourceName;

  private String uuid;
  private String type;
  private Map<String, Object> params;
  private String sourceUuid;
  private Map<String, Object> sourceParams;
  private String sourceType;
  private Map<String, Object> planParams;

  public CoreSearchIndex(final String name, final String sourceName) {
    this.name = name;
    this.sourceName = sourceName;
  }

  @JsonCreator
  public CoreSearchIndex(
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

  public static CoreSearchIndex fromJson(final String input) {
    try {
      return Mapper.decodeInto(input, CoreSearchIndex.class);
    } catch (Exception ex) {
      throw InvalidArgumentException.fromMessage("Could not decode search index JSON", ex);
    }
  }

  public String name() {
    return name;
  }

  public String uuid() {
    return uuid;
  }

  public CoreSearchIndex uuid(String uuid) {
    this.uuid = uuid;
    return this;
  }
  public CoreSearchIndex unsetUuid() {
    this.uuid = null;
    return this;
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

  public CoreSearchIndex params(Map<String, Object> params) {
    this.params = params;
    return this;
  }

  public String sourceUuid() {
    return sourceUuid;
  }

  public CoreSearchIndex sourceUuid(String sourceUuid) {
    this.sourceUuid = sourceUuid;
    return this;
  }

  public Map<String, Object> sourceParams() {
    return sourceParams;
  }

  public CoreSearchIndex sourceParams(Map<String, Object> sourceParams) {
    this.sourceParams = sourceParams;
    return this;
  }

  public String sourceType() {
    return sourceType;
  }

  public CoreSearchIndex sourceType(String sourceType) {
    this.sourceType = sourceType;
    return this;
  }

  public Map<String, Object> planParams() {
    return planParams;
  }

  public CoreSearchIndex planParams(Map<String, Object> planParams) {
    this.planParams = planParams;
    return this;
  }

  public String toJson() {
    Map<String, Object> output = new HashMap<>();
    // A UUID is server-assigned.  It must be specified on an update, and must not be specified on a create.
    if (uuid != null) {
      output.put("uuid", uuid);
    }
    output.put("name", name);
    output.put("sourceName", sourceName);
    output.put("type", type == null ? "fulltext-index" : type);
    output.put("sourceType", sourceType == null ? "couchbase" : sourceType);

    if (!isNullOrEmpty(params))  {
      output.put("params", params);
    }
    if (!isNullOrEmpty(planParams)) {
      output.put("planParams", planParams);
    }
    if (!isNullOrEmpty(sourceParams)) {
      output.put("sourceParams", sourceParams);
    }
    if (sourceUuid != null) {
      output.put("sourceUUID", sourceUuid);
    }

    return Mapper.encodeAsString(output);
  }

  boolean containsVectorMappings() {
    JsonNode json = Mapper.decodeIntoTree(toJson().getBytes(StandardCharsets.UTF_8));
    if (json.has("params")) {
      JsonNode params = json.get("params");
      if (params.isObject() && params.has("mapping")) {
        JsonNode mapping = params.get("mapping");
        if (mapping.isObject() && mapping.has("types")) {
          JsonNode types = mapping.get("types");
          if (types.isObject()) {
            Iterator<String> typesFieldNames = types.fieldNames();
            while (typesFieldNames.hasNext()) {
              String typesFieldName = typesFieldNames.next();
              JsonNode type = types.get(typesFieldName);
              if (type.isObject() && type.has("properties")) {
                JsonNode properties = type.get("properties");
                Iterator<String> propertiesFieldNames = properties.fieldNames();
                while (propertiesFieldNames.hasNext()) {
                  String propertiesFieldName = propertiesFieldNames.next();
                  JsonNode property = properties.get(propertiesFieldName);
                  if (property.has("fields")) {
                    JsonNode fields = property.get("fields");
                    if (fields.isArray()) {
                      ArrayNode fieldsArray = ((ArrayNode) fields);
                      for (int i = 0; i < fieldsArray.size(); i++) {
                        JsonNode field = fieldsArray.get(i);
                        if (field.has("type")) {
                          JsonNode typ = field.get("type");
                          if (typ.isTextual()) {
                            if (typ.textValue().equals("vector")) {
                              return true;
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "CoreSearchIndex{" +
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
