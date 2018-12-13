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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Allows to customize a get request.
 */
public class GetOptions extends CommonOptions<GetOptions> {

  /**
   * The default options, used most of the time.
   */
  public static final GetOptions DEFAULT = new GetOptions();

  /**
   * If the expiration should also fetched with a get.
   */
  private boolean withExpiration;

  /**
   * Holds a possible projection.
   */
  private List<String> projections;

  /**
   * Creates a new set of {@link GetOptions} with a {@link JsonObject} target.
   *
   * @return options to customize.
   */
  public static GetOptions getOptions() {
    return new GetOptions();
  }

  private GetOptions() {
    withExpiration = false;
  }

  public GetOptions withExpiration(boolean expiration) {
    withExpiration = true;
    return this;
  }

  public boolean withExpiration() {
    return withExpiration;
  }

  public GetOptions project(final String... paths) {
    if (projections == null) {
      projections = new ArrayList<>(paths.length);
    }
    Collections.addAll(projections, paths);
    return this;
  }

  @Stability.Internal
  public List<String> projections() {
    return projections;
  }

}
