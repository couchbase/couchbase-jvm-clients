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
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbCollections.copyToUnmodifiableList;
import static com.couchbase.client.core.util.CbCollections.copyToUnmodifiableSet;
import static com.couchbase.client.core.util.CbStrings.isNullOrEmpty;
import static com.couchbase.client.core.util.CbStrings.nullToEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Information sent by the server in response to a "get user(s)" request.
 * This includes the {@link User} record itself (which may be retrieved by calling
 * {@link #user()}) as well as metadata like which roles are inherited from which groups.
 */
@Stability.Volatile
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserAndMetadata {
  private final AuthDomain domain;
  private final Optional<Instant> passwordChanged;
  private final String username;
  private final String displayName;
  private final Set<String> groups;
  private final Set<String> externalGroups;
  private final List<RoleAndOrigins> effectiveRoles;

  public UserAndMetadata(@JsonProperty("domain") AuthDomain domain,
                         @JsonProperty("id") String username,
                         @JsonProperty("name") String displayName,
                         @JsonProperty("roles") Collection<RoleAndOrigins> effectiveRoles,
                         @JsonProperty("groups") Collection<String> groups,
                         @JsonProperty("external_groups") Collection<String> externalGroups,
                         @JsonProperty("password_change_date") String passwordChanged) {

    this.username = requireNonNull(username);
    this.displayName = nullToEmpty(displayName);
    this.groups = copyToUnmodifiableSet(groups);

    this.domain = requireNonNull(domain);
    this.effectiveRoles = copyToUnmodifiableList(effectiveRoles);
    this.passwordChanged = parseOptionalInstant(passwordChanged);
    this.externalGroups = copyToUnmodifiableSet(externalGroups);
  }

  public AuthDomain domain() {
    return domain;
  }

  /**
   * Returns a new mutable {@link User} with initial values matching the data in this {@link UserAndMetadata}.
   */
  public User user() {
    return new User(username)
        .displayName(displayName)
        .groups(groups)
        .roles(innateRoles());
  }

  /**
   * Returns the roles assigned specifically to the user. Excludes roles that are
   * only inherited from groups.
   */
  public Set<Role> innateRoles() {
    return effectiveRoles.stream()
        .filter(RoleAndOrigins::innate)
        .map(RoleAndOrigins::role)
        .collect(Collectors.toSet());
  }

  /**
   * Returns all of the user's roles, including roles inherited from groups.
   */
  public Set<Role> effectiveRoles() {
    return effectiveRoles.stream()
        .map(RoleAndOrigins::role)
        .collect(Collectors.toSet());
  }

  /**
   * Returns all of the user's roles plus information about whether each role
   * is assigned specifically to the user or inherited from a group, and if so which group.
   */
  public List<RoleAndOrigins> effectiveRolesAndOrigins() {
    return effectiveRoles;
  }

  public Set<String> externalGroups() {
    return externalGroups;
  }

  /**
   * Returns the time the user's password was last set, if known.
   */
  public Optional<Instant> passwordChanged() {
    return passwordChanged;
  }

  private static Optional<Instant> parseOptionalInstant(String passwordChanged) {
    return isNullOrEmpty(passwordChanged) ? Optional.empty() : Optional.of(Instant.parse(passwordChanged));
  }

  @Override
  public String toString() {
    return "UserAndMetadata{" +
        "domain=" + domain +
        ", user=" + user() +
        ", passwordChanged=" + passwordChanged +
        ", externalGroups=" + externalGroups +
        ", effectiveRoles=" + effectiveRoles +
        '}';
  }
}
