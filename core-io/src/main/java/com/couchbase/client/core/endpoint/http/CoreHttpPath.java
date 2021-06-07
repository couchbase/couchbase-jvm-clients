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

package com.couchbase.client.core.endpoint.http;

import com.couchbase.client.core.annotation.Stability;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

/**
 * The "path" component of a URI, with support for path parameters
 * whose replacement values are automatically url-encoded.
 */
@Stability.Internal
public class CoreHttpPath {
  private final String template;
  private final Map<String, String> params;
  private final String formatted;

  public static CoreHttpPath path(String template) {
    return new CoreHttpPath(template, emptyMap());
  }

  public static CoreHttpPath path(String template, Map<String, String> params) {
    return new CoreHttpPath(template, params);
  }

  private CoreHttpPath(String template, Map<String, String> params) {
    this.template = template.startsWith("/") ? template : "/" + template;
    this.params = requireNonNull(params);
    this.formatted = resolve(this.template, params);
  }

  public String getTemplate() {
    return template;
  }

  public Map<String, String> getParams() {
    return unmodifiableMap(params);
  }

  public String format() {
    return formatted;
  }

  /**
   * Returns a new path built by appending the given subpath to this path.
   */
  public CoreHttpPath plus(String subpath) {
    return plus(path(subpath));
  }

  /**
   * Returns a new path built by appending the given subpath template
   * and parameters to this path.
   */
  public CoreHttpPath plus(String subpathTemplate, Map<String, String> subpathParams) {
    return plus(path(subpathTemplate, subpathParams));
  }

  /**
   * Returns a new path built by appending the given subpath to this path.
   */
  public CoreHttpPath plus(CoreHttpPath subpath) {
    Set<String> commonParams = new HashSet<>(this.params.keySet());
    commonParams.retainAll(subpath.params.keySet());
    if (!commonParams.isEmpty()) {
      throw new IllegalArgumentException("Subpath must not have parameter names in common with base path, but found: " + commonParams);
    }

    Map<String, String> mergedParams = new HashMap<>(this.params);
    mergedParams.putAll(subpath.params);
    return new CoreHttpPath(this.template + subpath.template, mergedParams);
  }

  @Override
  public String toString() {
    return "CoreHttpPath{" +
        "template='" + template + '\'' +
        ", params=" + params +
        ", formatted='" + formatted + '\'' +
        '}';
  }

  private static String resolve(String template, Map<String, String> params) {
    String resolved = template;
    for (Map.Entry<String, String> param : params.entrySet()) {
      String key = param.getKey();
      String value = urlEncode(param.getValue());
      resolved = resolved.replace("{" + key + "}", value);
    }
    if (resolved.contains("{") && resolved.contains("}")) {
      throw new IllegalArgumentException("Path has unresolved placeholder: " + resolved);
    }
    return resolved;
  }
}
