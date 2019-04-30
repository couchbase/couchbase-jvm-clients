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

package com.couchbase.client.core.io.netty.kv;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonValue;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.DeserializationFeature;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectReader;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.couchbase.client.core.json.Mapper;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The {@link ErrorMap} contains mappings from errors to their attributes, negotiated
 * between the client and the server.
 *
 * <p>From the documentation:</p>
 *
 * <p>An Error Map is a mapping of error to their attributes and properties. It is used by
 * connected clients to handle error codes which they may otherwise not be aware of.</p>
 *
 * <p>The error map solves the problem where clients would incorrectly handle newer error codes,
 * and/or where the server would disconnect the client because it needed to send it a new
 * error code.</p>
 *
 * <p>The solution via Error Map is to establish a contract between client and server about certain
 * attributes which each error code may have. These attributes indicate whether an error may be
 * passed through, whether the command is retriable and so on. When a client receives an error code
 * it does not know about it, it can look up its attributes and then determine the next course of
 * action.</p>
 *
 * @since 1.4.4
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ErrorMap implements Comparable<ErrorMap> {

  private static final ObjectReader objectReader = Mapper.reader().forType(ErrorMap.class)
    .with(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL)
    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  private final int version;
  private final int revision;
  private final Map<Short, ErrorCode> errors;

  /**
   * Creates a new ErrorMap by parsing the json representation.
   *
   * @throws IOException if parsing failed
   */
  public static ErrorMap fromJson(byte[] jsonBytes) throws IOException {
    return objectReader.readValue(jsonBytes);
  }

  /**
   * Creates a new Error Map, usually called from jackson.
   *
   * @param version the error map version.
   * @param revision the error map revision.
   * @param errors the full error section.
   */
  @JsonCreator
  public ErrorMap(
    @JsonProperty("version") int version,
    @JsonProperty("revision") int revision,
    @JsonProperty("errors") Map<String, ErrorCode> errors) {
    this.version = version;
    this.revision = revision;
    this.errors = toShortKeys(errors);
  }

  private static Map<Short, ErrorCode> toShortKeys(Map<String, ErrorCode> errors) {
    Map<Short, ErrorCode> result = new HashMap<Short, ErrorCode>(errors.size());
    for (Map.Entry<String, ErrorCode> entry : errors.entrySet()) {
      result.put(Short.parseShort(entry.getKey(), 16), entry.getValue());
    }
    return result;
  }

  @Override
  public int compareTo(ErrorMap o) {
    if (version < o.version()) {
      return -1;
    } else if (version > o.version()) {
      return 1;
    }

    if (revision < o.revision()) {
      return -1;
    } else if (revision > o.revision()) {
      return 1;
    }

    return 0;
  }

  public int version() {
    return version;
  }

  public int revision() {
    return revision;
  }

  public Map<Short, ErrorCode> errors() {
    return errors;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ErrorCode {
    private final String name;
    private final String description;
    private final Set<ErrorAttribute> attributes;
    private final RetrySpecification retrySpec;

    /**
     * Creates a new error code, usually called from jackson.
     *
     * @param name name of the error code.
     * @param description description of the error code.
     * @param attributes the attributes for each error.
     * @param retrySpec the retry specification for the error code.
     */
    @JsonCreator
    public ErrorCode(
      @JsonProperty("name") String name,
      @JsonProperty("desc") String description,
      @JsonProperty("attrs") @JsonDeserialize(as = EnumSet.class) Set<ErrorAttribute> attributes,
      @JsonProperty("retry") RetrySpecification retrySpec) {
      this.name = name;
      this.description = description;
      this.attributes = attributes;
      this.retrySpec = retrySpec;
    }

    public String name() {
      return name;
    }

    public String description() {
      return description;
    }

    public Set<ErrorAttribute> attributes() {
      return attributes;
    }

    public RetrySpecification retrySpec() {
      return retrySpec;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("ErrorCode{");
      sb.append("name='" + name + '\'');
      sb.append(", description='" + description + '\'');
      sb.append(", attributes=" + attributes);
      if (retrySpec != null) {
        sb.append(", retryHint=" + retrySpec.toString());
      }
      sb.append("}");
      return sb.toString();
    }
  }

  public enum ErrorAttribute {
    /**
     * The operation was successful for those situations
     * where the error code is indicating successful (i.e. subdoc
     * operations carried out on a deleted document)
     */
    SUCCESS("success"),
    /**
     * This attribute means that the error is related to a constraint
     * failure regarding the item itself, i.e. the item does not exist,
     * already exists, or its current value makes the current operation
     * impossible. Retrying the operation when the item's value or status
     * has changed may succeed.
     */
    ITEM_ONLY("item-only"),
    /**
     *  This attribute means that a user's input was invalid because it
     *  violates the semantics of the operation, or exceeds some predefined
     *  limit.
     */
    INVALID_INPUT("invalid-input"),
    /**
     * The client's cluster map may be outdated and requires updating. The
     * client should obtain a newer configuration.
     */
    FETCH_CONFIG("fetch-config"),
    /**
     * The current connection is no longer valid. The client must reconnect
     * to the server. Note that the presence of other attributes may indicate
     * an alternate remedy to fixing the connection without a disconnect, but
     * without special remedial action a disconnect is needed.
     */
    CONN_STATE_INVALIDATED("conn-state-invalidated"),
    /**
     *  The operation failed because the client failed to authenticate or is not authorized
     *  to perform this operation. Note that this error in itself does not mean the connection
     *  is invalid, unless conn-state-invalidated is also present.
     */
    AUTH("auth"),
    /**
     * This error code must be handled specially. If it is not handled, the connection must be
     * dropped.
     */
    SPECIAL_HANDLING("special-handling"),
    /**
     * The operation is not supported, possibly because the of server version, bucket type, or
     * current user.
     */
    SUPPORT("support"),
    /**
     * This error is transient. Note that this does not mean the error is retriable.
     */
    TEMP("temp"),
    /**
     *  This is an internal error in the server.
     */
    INTERNAL("internal"),
    /**
     *  The operation may be retried immediately.
     */
    RETRY_NOW("retry-now"),
    /**
     * The operation may be retried after some time.
     */
    RETRY_LATER("retry-later"),
    /**
     * The error is related to the subdocument subsystem.
     */
    SUBDOC("subdoc"),
    /**
     * The error is related to the DCP subsystem.
     */
    DCP("dcp"),
    /**
     * Use retry specifications from the server.
     */
    AUTO_RETRY("auto-retry"),
    /**
     * This attribute specifies that the requested item is currently locked.
     */
    ITEM_LOCKED("item-locked"),
    /**
     * This attribute means that the error is related
     * to operating on a soft-deleted document.
     */
    ITEM_DELETED("item-deleted");

    private final String raw;

    ErrorAttribute(String raw) {
      this.raw = raw;
    }

    @JsonValue
    public String raw() {
      return raw;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RetrySpecification {
    private final RetryStrategy strategy;
    private final long interval;
    private final long after;
    private final long maxDuration;
    private final long ceil;

    /**
     * Creates a new retry specification, usually called from jackson.
     *
     * @param strategy the converted strategy.
     * @param interval the retry interval.
     * @param after after which time the retry should happen.
     * @param maxDuration the maximum duration of the retry.
     * @param ceil the ceiling of the retry delay.
     */
    @JsonCreator
    public RetrySpecification(
      @JsonProperty("strategy") RetryStrategy strategy,
      @JsonProperty("interval") int interval,
      @JsonProperty("after") int after,
      @JsonProperty("max-duration") int maxDuration,
      @JsonProperty("ceil") int ceil) {
      this.strategy = strategy;
      this.interval = interval;
      this.after = after;
      this.maxDuration = maxDuration;
      this.ceil = ceil;
    }

    public RetryStrategy strategy() {
      return this.strategy;
    }

    public long interval() {
      return this.interval;
    }

    public long after() {
      return this.after;
    }

    public long maxDuration() {
      return this.maxDuration;
    }

    public long ceil() {
      return this.ceil;
    }

    @Override
    public String toString() {
      return "Retry{"
        + "strategy=" + strategy()
        + ", interval=" + interval()
        + ", after=" + after()
        + ", max-duration=" + maxDuration()
        + ", ceil=" + ceil()
        + "}";
    }
  }

  public enum RetryStrategy {
    EXPONENTIAL("exponential"),
    LINEAR("linear"),
    CONSTANT("constant");

    private final String strategy;

    RetryStrategy(String strategy) {
      this.strategy = strategy;
    }

    @JsonValue
    public String strategy() {
      return strategy;
    }
  }

  @Override
  public String toString() {
    return "ErrorMap{"
      + "version=" + version
      + ", revision=" + revision
      + ", errors=" + errors
      + '}';
  }
}
