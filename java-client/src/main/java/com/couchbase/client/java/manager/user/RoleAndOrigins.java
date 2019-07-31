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
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.core.json.Mapper;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.couchbase.client.core.util.CbCollections.copyToUnmodifiableList;
import static com.couchbase.client.core.util.CbCollections.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Associates a {@link Role} with information about why a user has the role.
 */
@Stability.Volatile
@JsonIgnoreProperties(ignoreUnknown = true)
public class RoleAndOrigins {

  /**
   * Indicates why a user has the role.
   * An origin of type {@code "user"} means the role is assigned specifically to the user (in which case the {@code name} field is null.
   * An origin of type {@code "group"} means the role is inherited from the group identified by the {@code name} field.
   */
  public static class Origin {
    private static final Origin USER = new Origin("user", null);

    private final String type;
    private final Optional<String> name;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public Origin(@JsonProperty("type") String type,
                  @JsonProperty("name") String name) {
      this.type = requireNonNull(type);
      this.name = Optional.ofNullable(name);
    }

    public String type() {
      return type;
    }

    public Optional<String> name() {
      return name;
    }

    @Override
    public String toString() {
      return name.map(name -> type + ":" + name).orElse(type);
    }
  }

  private final Role role;
  private final List<Origin> origins;

  @JsonCreator
  public RoleAndOrigins(ObjectNode node) {
    this(Mapper.convertValue(node, Role.class),
        Mapper.convertValue(node.path("origins"), new TypeReference<List<Origin>>() {
        }));
  }

  public RoleAndOrigins(Role role, List<Origin> origins) {
    this.role = requireNonNull(role);

    // Couchbase versions prior to 6.5 do not return origins since all roles are implicitly innate to the user.
    this.origins = isNullOrEmpty(origins)
        ? Collections.singletonList(Origin.USER)
        : copyToUnmodifiableList(origins);
  }

  /**
   * Returns true if this role is assigned specifically to the user (has origin "user"
   * as opposed to being inherited from a group).
   */
  public boolean innate() {
    return origins.stream()
        .anyMatch(origin -> origin.type.equals("user"));
  }

  public Role role() {
    return role;
  }

  public List<Origin> origins() {
    return origins;
  }

  @Override
  public String toString() {
    return role + "<-" + origins;
  }
}
