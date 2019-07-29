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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.core.util.CbStrings.nullToEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Volatile
public class User {
  private final String username;
  private String displayName = "";
  private Set<String> groups = new HashSet<>();
  private Set<Role> innateRoles = new HashSet<>();
  private Optional<String> password = Optional.empty();

  public User(String username) {
    this.username = requireNonNull(username);
  }

  public String username() {
    return username;
  }

  Optional<String> password() {
    return password;
  }

  public User password(String newPassword) {
    this.password = Optional.of(newPassword);
    return this;
  }

  public String displayName() {
    return displayName;
  }

  public User displayName(String displayName) {
    this.displayName = nullToEmpty(displayName);
    return this;
  }

  public Set<String> groups() {
    return groups;
  }

  public User groups(Collection<String> groups) {
    this.groups = new HashSet<>(groups);
    return this;
  }

  public User groups(String... groups) {
    return groups(Arrays.asList(groups));
  }

  public Set<Role> roles() {
    return innateRoles;
  }

  public User roles(Collection<Role> roles) {
    this.innateRoles = new HashSet<>(roles);
    return this;
  }

  public User roles(Role... roles) {
    return roles(Arrays.asList(roles));
  }

  @Override
  public String toString() {
    return "User{" +
        "username='" + username + '\'' +
        ", displayName='" + displayName + '\'' +
        ", groups=" + groups +
        ", innateRoles=" + innateRoles +
        '}';
  }
}
