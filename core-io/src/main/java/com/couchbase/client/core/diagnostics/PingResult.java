/*
 * Copyright (c) 2017 Couchbase, Inc.
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
package com.couchbase.client.core.diagnostics;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.core.error.EncodingFailureException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.service.ServiceType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public class PingResult {

  /**
   * The version is used when exporting to JSON.
   */
  static final int VERSION = 2;

  /**
   * Holds the individual health of each endpoint in the report.
   */
  private final Map<ServiceType, List<EndpointPingReport>> endpoints;

  /**
   * The SDK identifier used.
   */
  private final String sdk;

  /**
   * The report ID (either user provided or auto generated).
   */
  private final String id;

  /**
   * Creates the new ping report.
   * <p>
   * Note that this is internal API and should not be called at the application level.
   *
   * @param endpoints the health of each individual endpoint.
   * @param sdk the sdk identifier.
   * @param id the report ID.
   */
  @Stability.Internal
  public PingResult(final Map<ServiceType, List<EndpointPingReport>> endpoints, final String sdk,
                    final String id) {
    this.id = id == null ? UUID.randomUUID().toString() : id;
    this.endpoints = endpoints;
    this.sdk = sdk;
  }

  /**
   * The ID of this report.
   *
   * @return the ID, either automatically generated or the one provided by the user.
   */
  public String id() {
    return id;
  }

  /**
   * The version of this report (useful when exporting to JSON).
   *
   * @return the version format of this report.
   */
  public int version() {
    return VERSION;
  }

  /**
   * The identifier of this SDK (useful when exporting to JSON).
   *
   * @return the identifier of this SDK.
   */
  public String sdk() {
    return sdk;
  }

  /**
   * Returns the diagnostic reports of each individual endpoint.
   */
  public Map<ServiceType, List<EndpointPingReport>> endpoints() {
    return endpoints;
  }

  /**
   * Exports this report into the standard JSON format which is consistent across different SDKs.
   *
   * @return the report encoded as JSON.
   */
  public String exportToJson() {
    final Map<String, Object> result = new HashMap<>();
    final Map<String, List<Map<String, Object>>> services = new HashMap<>();

    for (final Map.Entry<ServiceType, List<EndpointPingReport>> e : endpoints.entrySet()) {
      services.put(
        e.getKey().ident(),
        e.getValue().stream().map(EndpointPingReport::toMap).collect(Collectors.toList())
      );
    }

    result.put("version", VERSION);
    result.put("services", services);
    result.put("sdk", sdk);
    result.put("id", id);

    try {
      return Mapper.writer().writeValueAsString(result);
    } catch (JsonProcessingException e) {
      throw new EncodingFailureException("Could not encode report to JSON.", e);
    }
  }

  @Override
  public String toString() {
    return "PingResult{" +
      "endpoints=" + endpoints +
      ", version=" + VERSION +
      ", sdk='" + sdk + '\'' +
      ", id='" + id + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PingResult that = (PingResult) o;
    return Objects.equals(endpoints, that.endpoints) &&
      Objects.equals(sdk, that.sdk) &&
      Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(endpoints, sdk, id);
  }
}
