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

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Volatile
public class BucketNotFoundException extends CouchbaseException {

  private final String bucketName;

  public BucketNotFoundException(String bucketName) {
    this(bucketName, null);
  }

  public BucketNotFoundException(String bucketName, ErrorContext errorContext) {
    super("Bucket" + formatCollection(bucketName) + " not found.", errorContext);
    this.bucketName = requireNonNull(bucketName);
  }

  private static String formatCollection(String collectionName) {
    return isNullOrEmpty(collectionName) ? "" : " [" + redactMeta(collectionName) + "]";
  }

  public static BucketNotFoundException forBucket(final String bucketName) {
    return new BucketNotFoundException(bucketName);
  }

  public String bucketName() {
    return bucketName;
  }
}
