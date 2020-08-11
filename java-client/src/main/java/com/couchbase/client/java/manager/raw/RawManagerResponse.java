/*
 * Copyright 2020 Couchbase, Inc.
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

package com.couchbase.client.java.manager.raw;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.TypeRef;

import java.nio.charset.StandardCharsets;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

@Stability.Uncommitted
public class RawManagerResponse {

  private final ServiceType serviceType;
  private final JsonSerializer serializer;
  private final int httpStatus;
  private final byte[] payload;

  public RawManagerResponse(ServiceType serviceType, JsonSerializer serializer, int httpStatus, byte[] payload) {
    this.serviceType = serviceType;
    this.serializer = serializer;
    this.httpStatus = httpStatus;
    this.payload = payload;
  }

  /**
   * Returns the HTTP status code returned from the cluster.
   */
  public int httpStatus() {
    return httpStatus;
  }

  /**
   * Returns the service type this response has been dispatched against.
   */
  public ServiceType serviceType() {
    return serviceType;
  }

  /**
   * Converts the payload into the target format.
   *
   * @param target the target class to deserialize into.
   * @throws DecodingFailureException if any row could not be successfully deserialized.
   */
  public <T> T contentAs(final Class<T> target) {
    return serializer.deserialize(target, payload);
  }

  /**
   * Converts the payload into the target format.
   *
   * @param target the target class to deserialize into.
   * @throws DecodingFailureException if any row could not be successfully deserialized.
   */
  public <T> T contentAs(final TypeRef<T> target) {
    return serializer.deserialize(target, payload);
  }

  @Override
  public String toString() {
    return "RawManagerResponse{" +
      "serviceType=" + serviceType +
      ", httpStatus=" + httpStatus +
      ", payload=" + redactMeta(new String(payload, StandardCharsets.UTF_8)) +
      '}';
  }
}
