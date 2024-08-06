/*
 * Copyright 2024 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.columnar.client.java;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.util.Golang;
import com.couchbase.columnar.client.java.codec.Deserializer;
import com.couchbase.columnar.client.java.internal.InternalJacksonSerDes;
import com.couchbase.columnar.client.java.internal.JacksonTransformers;
import com.couchbase.columnar.client.java.internal.JsonSerializer;
import com.couchbase.columnar.client.java.json.JsonArray;
import com.couchbase.columnar.client.java.json.JsonObject;
import org.jetbrains.annotations.ApiStatus.Experimental;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Optional parameters for {@link Queryable#executeQuery(String, Consumer)}
 * and {@link Queryable#executeStreamingQuery(String, Consumer, Consumer)}.
 */
public final class QueryOptions {
  @Nullable private Duration timeout;
  @Nullable private String clientContextId;
  @Nullable private Map<String, ?> namedParameters;
  @Nullable private List<?> positionalParameters;
  @Nullable private QueryPriority priority;
  @Nullable private ScanConsistency scanConsistency;
  @Nullable private Duration scanWait;
  @Nullable private Deserializer deserializer;
  @Nullable private Boolean readOnly;
  @Nullable private Map<String, ?> raw;

  public QueryOptions timeout(@Nullable Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  public QueryOptions parameters(@Nullable Map<String, ?> namedParameters) {
    this.namedParameters = namedParameters == null ? null : unmodifiableMap(new HashMap<>(namedParameters));
    return this;
  }

  /**
   * @implNote There must not be a varargs overload; that would
   * introduce ambiguity, since a query can have a single parameter
   * whose value is a list.
   */
  public QueryOptions parameters(@Nullable List<?> positionalParameters) {
    this.positionalParameters = positionalParameters == null ? null : unmodifiableList(new ArrayList<>(positionalParameters));
    return this;
  }

  /**
   * Sets the deserializer used by {@link Row#as} to convert query result rows into Java objects.
   * <p>
   * If not specified, defaults to the cluster's default deserializer.
   *
   * @see ClusterOptions#deserializer(Deserializer)
   */
  public QueryOptions deserializer(@Nullable Deserializer deserializer) {
    this.deserializer = deserializer;
    return this;
  }

  public QueryOptions scanWait(@Nullable Duration scanWait) {
    this.scanWait = scanWait;
    return this;
  }

  public QueryOptions priority(@Nullable QueryPriority priority) {
    this.priority = priority;
    return this;
  }

  public QueryOptions scanConsistency(@Nullable ScanConsistency scanConsistency) {
    this.scanConsistency = scanConsistency;
    return this;
  }

  public QueryOptions readOnly(@Nullable Boolean readOnly) {
    this.readOnly = readOnly;
    return this;
  }

  /**
   * Specifies arbitrary name-value pairs to include the query request JSON.
   * <p>
   * Marked as "experimental" because this might not be supported by future
   * client-server protocols. Let us know if you need to use this method,
   * so we can consider extending the stable API to support your use case.
   */
  @Experimental
  public QueryOptions raw(@Nullable Map<String, ?> raw) {
    this.raw = raw == null ? null : unmodifiableMap(new HashMap<>(raw));
    return this;
  }

  Unmodifiable build() {
    return new Unmodifiable(this);
  }

  static class Unmodifiable {
    @Nullable private final Duration timeout;
    @Nullable private final String clientContextId;
    @Nullable private final Map<String, ?> namedParameters;
    @Nullable private final List<?> positionalParameters;
    @Nullable private final QueryPriority priority;
    @Nullable private final ScanConsistency scanConsistency;
    @Nullable private final Duration scanWait;
    @Nullable private final Deserializer deserializer;
    @Nullable private final Boolean readOnly;
    @Nullable private final Map<String, ?> raw;

    Unmodifiable(QueryOptions builder) {
      this.timeout = builder.timeout;
      this.clientContextId = builder.clientContextId;
      this.namedParameters = builder.namedParameters;
      this.positionalParameters = builder.positionalParameters;
      this.priority = builder.priority;
      this.scanConsistency = builder.scanConsistency;
      this.scanWait = builder.scanWait;
      this.deserializer = builder.deserializer;
      this.readOnly = builder.readOnly;
      this.raw = builder.raw;
    }

    void injectParams(ObjectNode query) {
      query.put(
        "client_context_id",
        clientContextId != null ? clientContextId : UUID.randomUUID().toString()
      );

      if (scanConsistency != null) {
        query.put("scan_consistency", scanConsistency.toString());
      }

      if (
        scanWait != null
          && scanConsistency != null
          && scanConsistency != ScanConsistency.NOT_BOUNDED
      ) {
        query.put("scan_wait", Golang.encodeDurationToMs(scanWait));
      }

      boolean positionalPresent = positionalParameters != null && !positionalParameters.isEmpty();
      if (positionalPresent) {
        try {
          JsonNode jsonArray = JacksonTransformers.MAPPER.convertValue(
            JsonArray.from(positionalParameters), // ensures internal serializer can safely handle the values
            JsonNode.class
          );
          query.set("args", jsonArray);
        } catch (Exception e) {
          throw new IllegalArgumentException("Unsupported parameter type.", e);
        }
      }

      boolean namedParametersPresent = namedParameters != null && !namedParameters.isEmpty();
      if (namedParametersPresent) {
        try {
          // ignore result; just ensure internal serializer can safely handle the values
          JsonObject.from(namedParameters);

          namedParameters.forEach((key, value) -> {
            JsonNode jsonValue = toRepackagedJacksonNode(InternalJacksonSerDes.INSTANCE, value);
            if (key.charAt(0) != '$') {
              query.set('$' + key, jsonValue);
            } else {
              query.set(key, jsonValue);
            }
          });
        } catch (Exception e) {
          throw new IllegalArgumentException("Unsupported parameter type.", e);
        }
      }

      if (readOnly != null) {
        query.put("readonly", readOnly);
      }

      if (raw != null) {
        // ignore result; just ensure internal serializer can safely handle the values
        JsonObject.from(raw);

        raw.forEach((key, value) -> {
          JsonNode jsonValue = toRepackagedJacksonNode(InternalJacksonSerDes.INSTANCE, value);
          query.set(key, jsonValue);
        });
      }
    }

    @Override
    public String toString() {
      return "QueryOptions{" +
        "timeout=" + timeout +
        ", clientContextId='" + clientContextId + '\'' +
        ", namedParameters=" + namedParameters +
        ", positionalParameters=" + positionalParameters +
        ", priority=" + priority +
        ", scanConsistency=" + scanConsistency +
        ", scanWait=" + scanWait +
        ", deserializer=" + deserializer +
        ", readOnly=" + readOnly +
        ", raw=" + raw +
        '}';
    }

    /**
     * Converts the given value to JSON using the given serializer,
     * then parses the JSON into a {@link JsonNode}.
     * <p>
     * This allows the user to specify query parameters as POJOs
     * and have them serialized using their chosen serializer.
     */
    private static JsonNode toRepackagedJacksonNode(
      JsonSerializer serializer,
      Object value
    ) {
      byte[] jsonArrayBytes = serializer.serialize(value);
      return Mapper.decodeIntoTree(jsonArrayBytes);
    }

    @Nullable
    public QueryPriority priority() {
      return priority;
    }

    public boolean readOnly() {
      return readOnly != null && readOnly;
    }

    public Map<String, Object> clientContext() {
      return emptyMap();
    }

    @Nullable
    public Duration timeout() {
      return timeout;
    }

    @Nullable
    public String clientContextId() {
      return clientContextId;
    }

    @Nullable
    public Map<String, ?> namedParameters() {
      return namedParameters;
    }

    @Nullable
    public List<?> positionalParameters() {
      return positionalParameters;
    }

    @Nullable
    public ScanConsistency scanConsistency() {
      return scanConsistency;
    }

    @Nullable
    public Duration scanWait() {
      return scanWait;
    }

    @Nullable
    public Deserializer deserializer() {
      return deserializer;
    }

  }
}
