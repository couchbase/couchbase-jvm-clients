/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.core.error;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonAnySetter;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.json.Mapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.core.util.CbStrings.nullToEmpty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * A numeric error code and associated human-readable error message.
 */
public class ErrorCodeAndMessage {
  private final int code;
  private final String message;

  // Jackson adds unrecognized properties here. An example is the "query_from_user" field
  // that appears in some query errors.
  @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
  @JsonAnySetter
  private final Map<String, Object> context = new HashMap<>();

  public ErrorCodeAndMessage(@JsonProperty("code") int code,
                             @JsonProperty("msg") String message) {
    this.code = code;
    this.message = nullToEmpty(message);
  }

  public int code() {
    return code;
  }

  public String message() {
    return message;
  }

  /**
   * Returns an unmodifiable map of any additional information returned by the server.
   */
  public Map<String, Object> context() {
    return unmodifiableMap(context);
  }

  @Override
  public String toString() {
    return code + " " + message + (!context.isEmpty() ? " Context: " + context : "");
  }

  @Stability.Internal
  public static List<ErrorCodeAndMessage> fromJsonArray(byte[] jsonArray) {
    return from(jsonArray);
  }

  private static final Pattern plaintextErrorPattern = Pattern.compile("(?<errorCode>\\d+):(?<message>.+)");

  private static List<ErrorCodeAndMessage> fromPlaintext(byte[] errorBytes) {
    String error = new String(errorBytes, UTF_8).trim();
    error = translateLegacyAnalyticsErrorCodes(error);

    Matcher m = plaintextErrorPattern.matcher(error);
    if (m.matches()) {
      return listOf(new ErrorCodeAndMessage(Integer.parseInt(m.group("errorCode")), m.group("message")));
    }
    return listOf(new ErrorCodeAndMessage(0, "Failed to decode error: " + error));
  }

  private static String translateLegacyAnalyticsErrorCodes(String error) {
    // Before 7.0 the analytics service returned non-numeric error codes
    // in link management API responses. Let's translate the ones we care about!

    if (error.startsWith("CBAS") || error.startsWith("ASX")) {
      return error
          .replace("CBAS0026", "24055") // link already exists
          .replace("CBAS0027", "24006") // link not found
          .replace("ASX1063", "24034") // dataverse not found

          // 24001 is arbitrary... just needs to be in the 24xxx range and not mapped to another exception type
          .replace("CBAS0062", "24001") // invalid argument (missing parameter)
          .replace("CBAS0063", "24001") // invalid argument (unexpected parameter)
          ;
    }
    return error;
  }

  @Stability.Internal
  public static List<ErrorCodeAndMessage> from(byte[] content) {
    JsonNode node;
    try {
      node = Mapper.decodeIntoTree(content);
    } catch (Exception notJson) {
      return fromPlaintext(content);
    }

    try {
      if (node.isArray()) {
        return unmodifiableList(Mapper.convertValue(node, new TypeReference<List<ErrorCodeAndMessage>>() {
        }));
      }
      if (node.isObject()) {
        return listOf(Mapper.convertValue(node, ErrorCodeAndMessage.class));
      }
    } catch (Exception malformed) {
      // fall through
    }

    return listOf(new ErrorCodeAndMessage(0, "Failed to decode errors: " + new String(content, UTF_8)));
  }
}
