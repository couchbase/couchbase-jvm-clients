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
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.json.JsonObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static com.couchbase.client.core.util.Validators.notNull;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;

/**
 * Allows to customize a get request.
 *
 * @since 3.0.0
 */
public class GetOptions extends CommonOptions<GetOptions> {

  /**
   * If the expiration should also fetched with a get.
   */
  private boolean withExpiry;

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

  /**
   * Holds the transcoder used for decoding.
   */
  private Transcoder transcoder;

  private GetOptions() {
    withExpiry = false;
  }

  /**
   * If set to true, the get will fetch the expiry for the document as well and return
   * it as part of the {@link GetResult}.
   *
   * @param expiry true if it should be fetched.
   * @return the {@link GetOptions} to allow method chaining.
   */
  public GetOptions withExpiry(boolean expiry) {
    withExpiry = expiry;
    return this;
  }

  /**
   * Allows to specify a custom list paths to fetch from the document instead of the whole.
   * <p>
   * Note that a maximum of 16 individual paths can be projected at a time due to a server limitation. If you need
   * more than that, think about fetching less-generic paths or the full document straight away.
   *
   * @param path a path that should be loaded if present.
   * @param morePaths additional paths that should be loaded if present.
   * @return the {@link GetOptions} to allow method chaining.
   */
  public GetOptions project(final String path, final String... morePaths) {
    return project(singletonList(path))
        .project(Arrays.asList(morePaths));
  }

  /**
   * Allows to specify a custom list paths to fetch from the document instead of the whole.
   * <p>
   * Note that a maximum of 16 individual paths can be projected at a time due to a server limitation. If you need
   * more than that, think about fetching less-generic paths or the full document straight away.
   *
   * @param paths each individual path that should be loaded if present.
   * @return the {@link GetOptions} to allow method chaining.
   */
  public GetOptions project(final Iterable<String> paths) {
    if (projections == null) {
      projections = new ArrayList<>();
    }

    for (String path : paths) {
      if (!isNullOrEmpty(path)) {
        projections.add(path);
      }
    }

    return this;
  }

  /**
   * Allows to specify a custom transcoder that is used to decode the content of the result.
   *
   * @param transcoder the custom transcoder that should be used for decoding.
   * @return the {@link GetOptions} to allow method chaining.
   */
  public GetOptions transcoder(final Transcoder transcoder) {
    notNull(transcoder, "Transcoder");
    this.transcoder = transcoder;
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  @Stability.Internal
  public class Built extends BuiltCommonOptions {

    public boolean withExpiry() {
      return withExpiry;
    }

    public List<String> projections() {
      return projections == null ? emptyList() : projections;
    }

    public Transcoder transcoder() {
      return transcoder;
    }

  }

}
