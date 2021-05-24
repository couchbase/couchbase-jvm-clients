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

import java.util.Map;

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
