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

import static com.couchbase.client.core.util.CbStrings.nullToEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Volatile
@JsonIgnoreProperties(ignoreUnknown = true)
public class RoleAndDescription {

  private final Role role;
  private final String displayName;
  private final String description;

  public RoleAndDescription(Role role, String displayName, String description) {
    this.role = requireNonNull(role);

    // These values should always be non-null, but a missing description isn't the end of the world
    this.displayName = nullToEmpty(displayName);
    this.description = nullToEmpty(description);
  }

  @JsonCreator
  private RoleAndDescription(@JsonProperty("role") String name,
                             @JsonProperty("bucket_name") String bucket,
                             @JsonProperty("scope_name") String scope,
                             @JsonProperty("collection_name") String collection,
                             @JsonProperty("name") String displayName,
                             @JsonProperty("desc") String description) {
    this(new Role(name, bucket, scope, collection), displayName, description);
  }

  public Role role() {
    return role;
  }

  public String displayName() {
    return displayName;
  }

  public String description() {
    return description;
  }

  @Override
  public String toString() {
    return role.toString() + "(" + displayName() + ": " + description() + ")";
  }
}
