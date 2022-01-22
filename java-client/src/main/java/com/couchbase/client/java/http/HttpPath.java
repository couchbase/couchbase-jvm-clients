/*
 * Copyright 2022 Couchbase, Inc.
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

package com.couchbase.client.java.http;

import com.couchbase.client.core.endpoint.http.CoreHttpPath;

import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Specifies the path for an HTTP request.
 * <p>
 * May also include the query string, if desired.
 * <p>
 * Create new instances using the static factory method.
 *
 * @see #of(String, String...)
 */
public class HttpPath {
  final String formatted;

  private HttpPath(String formatted) {
    this.formatted = requireNonNull(formatted);
  }

  /**
   * Returns a new path, substituting any "{}" placeholders in the template with
   * the corresponding argument.
   * <p>
   * Placeholder values are automatically URL-encoded.
   * <p>
   * Example usage:
   * <pre>
   * HttpPath path = HttpPath.of("/foo/{}/bar/{}", "hello world", "xyzzy");
   * System.out.println(path);
   * </pre>
   * Output:
   * <pre>
   * /foo/hello%20world/bar/xyzzy
   * </pre>
   *
   * @see HttpPath#of(String, List)
   *
   * @param template The template string, which may include "{}" placeholders.
   * @param args un-encoded values to substitute for the placeholders.
   * Values are automatically URL-encoded during the substitution process.
   * @throws IllegalArgumentException if the number of placeholders does not match the number of arguments.
   */
  public static HttpPath of(String template, String... args) {
    return HttpPath.of(template, Arrays.asList(args));
  }

  /**
   * Like {@link HttpPath#of(String, String...)} but takes the arguments as a list instead of varargs.
   *
   * @see HttpPath#of(String, String...)
   */
  public static HttpPath of(String template, List<String> args) {
    return new HttpPath(CoreHttpPath.formatPath(template, args));
  }

  @Override
  public String toString() {
    return formatted;
  }
}
