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

public class LookupInOptions extends CommonOptions<LookupInOptions> {

  /**
   * If the expiration should also be fetched.
   */
  private boolean withExpiration = false;

  public static LookupInOptions lookupInOptions() {
    return new LookupInOptions();
  }

  /**
   * If set to true, the get will fetch the expiration for the document as well and return
   * it as part of the {@link LookupInResult}.
   *
   * @param expiration true if it should be fetched.
   * @return the {@link LookupInOptions} to allow method chaining.
   */
  public LookupInOptions withExpiration(boolean expiration) {
    withExpiration = expiration;
    return this;
  }

  private LookupInOptions() {
  }

  @Stability.Internal
  public Built build() {
    return new Built();
  }

  public class Built extends BuiltCommonOptions {
    public boolean withExpiration() {
      return withExpiration;
    }
  }

}
