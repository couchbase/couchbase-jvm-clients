/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.api.manager;

import com.couchbase.client.core.annotation.Stability;

import static java.util.Objects.requireNonNull;

@Stability.Internal
public class CoreBucketAndScope {
  private final String bucketName;
  private final String scopeName;

  public CoreBucketAndScope(String bucketName, String scopeName) {
    this.scopeName = requireNonNull(scopeName);
    this.bucketName = requireNonNull(bucketName);
  }

  public String scopeName() {
    return scopeName;
  }

  public String bucketName() {
    return bucketName;
  }
}
