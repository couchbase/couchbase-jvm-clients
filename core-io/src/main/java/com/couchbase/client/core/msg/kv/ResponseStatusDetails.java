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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Objects;

/**
 * Container for extended response status information.
 *
 * @author Michael Nitschinger
 * @since 1.4.7
 */
public class ResponseStatusDetails {

  private static final TypeReference<HashMap<String,HashMap<String, String>>> JACKSON_TYPEREF
    = new TypeReference<HashMap<String,HashMap<String, String>>>() {};

  private final String reference;
  private final String context;

  /**
   * Helper method to convert a {@link ByteBuf} input into the details.
   *
   * It will NOT release the buffer.
   */
  public static ResponseStatusDetails convert(final byte[] input) {
    if (input.length == 0) {
      return null;
    }

    try {
      HashMap<String,HashMap<String, String>> result = Mapper.decodeInto(input, JACKSON_TYPEREF);
      HashMap<String, String> errorMap = result.get("error");
      if (errorMap == null) {
        return null;
      }
      return new ResponseStatusDetails(errorMap.get("ref"), errorMap.get("context"));
    } catch (Exception ex) {
      return null;
    }
  }

  ResponseStatusDetails(final String reference, final String context) {
    this.reference = reference;
    this.context = context;
  }

  public String reference() {
    return reference;
  }

  public String context() {
    return context;
  }

  @Override
  public String toString() {
    return "ResponseStatusDetails{" +
      "reference='" + reference + '\'' +
      ", context='" + context + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ResponseStatusDetails that = (ResponseStatusDetails) o;

    if (!Objects.equals(reference, that.reference)) return false;
    return Objects.equals(context, that.context);
  }

  @Override
  public int hashCode() {
    int result = reference != null ? reference.hashCode() : 0;
    result = 31 * result + (context != null ? context.hashCode() : 0);
    return result;
  }

  /**
   * Stringify the status details and the status in a best effort manner.
   */
  public static String stringify(final ResponseStatus status, final ResponseStatusDetails details) {
    String result = status.toString();
    if (details != null) {
      result = result + " (Context: " + details.context() + ", Reference: " + details.reference() + ")";
    }
    return result;
  }

}
