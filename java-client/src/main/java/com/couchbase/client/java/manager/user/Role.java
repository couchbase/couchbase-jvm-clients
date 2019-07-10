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

package com.couchbase.client.java.manager.user;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Stability.Volatile
@JsonIgnoreProperties(ignoreUnknown = true)
public class Role {

  private final String name;
  private final Optional<String> bucket;

  public Role(String roleName) {
    this(roleName, null);
  }

  @JsonCreator
  public Role(@JsonProperty("role") String roleName,
              @JsonProperty("bucket_name") String bucket) {
    this.name = requireNonNull(roleName);
    this.bucket = Optional.ofNullable(bucket);
  }

  public String name() {
    return name;
  }

  public Optional<String> bucket() {
    return bucket;
  }

  @Override
  public String toString() {
    return format();
  }

  public String format() {
    return bucket().isPresent() ? name + "[" + bucket().get() + "]" : name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Role role = (Role) o;
    return name.equals(role.name) &&
        Objects.equals(bucket, role.bucket);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, bucket);
  }
}
