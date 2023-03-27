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

package com.couchbase.client.core.error;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.context.ErrorContext;
import reactor.util.annotation.Nullable;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static java.util.Objects.requireNonNull;

@Stability.Volatile
public class BucketExistsException extends CouchbaseException {

  private final String bucketName;

  @Stability.Internal
  public BucketExistsException(final String bucketName, @Nullable ErrorContext context) {
    super("Bucket [" + redactMeta(bucketName) + "] already exists.", context);
    this.bucketName = requireNonNull(bucketName);
  }

  public static BucketExistsException forBucket(final String bucketName) {
    return new BucketExistsException(bucketName, null);
  }

  public String bucketName() {
    return bucketName;
  }
}
