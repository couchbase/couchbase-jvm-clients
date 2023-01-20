/*
 * Copyright 2019 Couchbase, Inc.
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

package com.couchbase.client.java.manager.bucket;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.java.CommonCreateOptions;

public class CreateBucketOptions extends CommonCreateOptions<CreateBucketOptions> {
  public static CreateBucketOptions createBucketOptions() {
    return new CreateBucketOptions();
  }

  private CreateBucketOptions() {
  }

  @Stability.Internal
  public CreateBucketOptions.Built build() {
    return new Built();
  }

  public class Built extends BuildCreateOptions {
    Built() { }
  }
}
