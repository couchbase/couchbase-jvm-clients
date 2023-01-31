/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.core.msg.kv;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.error.CouchbaseException;
import reactor.util.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class SubDocumentField implements Serializable {

  private static final long serialVersionUID = 227930811730226484L;

  private final SubDocumentOpResponseStatus status;
  @Nullable private final CouchbaseException error;
  private final byte[] value;
  private final String path;
  private final SubdocCommandType type;

  public SubDocumentField(SubDocumentOpResponseStatus status, Optional<CouchbaseException> error, byte[] value,
                          String path, SubdocCommandType type) {
    this.status = requireNonNull(status);
    this.error = error.orElse(null);
    this.value = requireNonNull(value);
    this.path = requireNonNull(path);
    this.type = requireNonNull(type);
  }

  public SubDocumentOpResponseStatus status() {
    return status;
  }

  public Optional<CouchbaseException> error() {
    return Optional.ofNullable(error);
  }

  public void throwErrorIfPresent() {
    if (error != null) {
      throw error;
    }
  }

  public byte[] value() {
    return value;
  }

  public String path() {
    return path;
  }

  public SubdocCommandType type() {
    return type;
  }

  @Override
  public String toString() {
    return "SubdocField{" +
      "status=" + status +
      ", value=" + new String(value, UTF_8) +
      ", path='" + path + '\'' +
      '}';
  }

}
