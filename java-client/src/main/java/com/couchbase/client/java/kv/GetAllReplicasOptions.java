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
import com.couchbase.client.core.api.kv.CoreReadPreference;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.json.JsonObject;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.util.Validators.notNull;

public class GetAllReplicasOptions extends CommonOptions<GetAllReplicasOptions> {

  /**
   * Holds the transcoder used for decoding.
   */
  private Transcoder transcoder;
  private CoreReadPreference readPreference;

  /**
   * Creates a new set of {@link GetAllReplicasOptions}.
   *
   * @return options to customize.
   */
  public static GetAllReplicasOptions getAllReplicasOptions() {
    return new GetAllReplicasOptions();
  }

  private GetAllReplicasOptions() {
  }

  /**
   * Allows to specify a custom transcoder that is used to decode the content of the result.
   *
   * @param transcoder the custom transcoder that should be used for decoding.
   * @return the {@link GetOptions} to allow method chaining.
   */
  public GetAllReplicasOptions transcoder(final Transcoder transcoder) {
    notNull(transcoder, "Transcoder");
    this.transcoder = transcoder;
    return this;
  }

  /**
   * Set a read preference for this operation.
   *
   * @see ReadPreference
   * @return this to allow method chaining.
   */
  public GetAllReplicasOptions readPreference(ReadPreference readPreference) {
    this.readPreference = readPreference.toCore();
    return this;
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {

    Built() { }

    public Transcoder transcoder() {
      return transcoder;
    }

    public @Nullable CoreReadPreference readPreference() {
      return readPreference;
    }
  }

}
