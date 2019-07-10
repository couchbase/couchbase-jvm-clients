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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.core.util.CbStrings.emptyToNull;
import static com.couchbase.client.core.util.CbStrings.nullToEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Volatile
@JsonIgnoreProperties(ignoreUnknown = true)
public class Group {
  private final String name;
  private String description = "";
  private Set<Role> roles = new HashSet<>();
  private Optional<String> ldapGroupReference = Optional.empty();

  public Group(String name) {
    this.name = requireNonNull(name);
  }

  @JsonCreator
  public Group(@JsonProperty("id") String name,
               @JsonProperty("description") String description,
               @JsonProperty("roles") Collection<Role> roles,
               @JsonProperty("ldap_group_ref") String ldapGroupReference) {
    this(name);

    description(description);
    roles(roles);
    ldapGroupReference(ldapGroupReference);
  }

  public String name() {
    return name;
  }

  public String description() {
    return description;
  }

  public Group description(String description) {
    this.description = nullToEmpty(description);
    return this;
  }

  public Set<Role> roles() {
    return roles;
  }

  public Group roles(Collection<Role> roles) {
    this.roles = new HashSet<>(roles);
    return this;
  }

  public Group roles(Role... roles) {
    return roles(Arrays.asList(roles));
  }

  public Optional<String> ldapGroupReference() {
    return ldapGroupReference;
  }

  public Group ldapGroupReference(String ldapGroupReference) {
    this.ldapGroupReference = Optional.ofNullable(emptyToNull(ldapGroupReference));
    return this;
  }

  @Override
  public String toString() {
    return "Group{" +
        "name='" + name + '\'' +
        ", description='" + description + '\'' +
        ", roles=" + roles +
        ", ldapGroupReference=" + ldapGroupReference +
        '}';
  }
}
