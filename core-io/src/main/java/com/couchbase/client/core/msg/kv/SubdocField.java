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

import com.couchbase.client.core.error.subdoc.SubDocumentException;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

// TODO rename SubDocumentField as part of public interface now
public class SubdocField {
  private final SubDocumentOpResponseStatus status;
  private final Optional<SubDocumentException> error;
  private final byte[] value;
  private final String path;
  private final SubdocCommandType type;

  public SubdocField(SubDocumentOpResponseStatus status,
                     Optional<SubDocumentException> error,
                     byte[] value,
                     String path,
                     SubdocCommandType type) {
    this.status = status;
    this.error = error;
    this.value = value;
    this.path = path;
    this.type = type;
  }

  public SubDocumentOpResponseStatus status() {
    return status;
  }

  public Optional<SubDocumentException> error() {
    return error;
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
