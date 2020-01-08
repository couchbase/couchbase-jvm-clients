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

import java.util.List;

import static com.couchbase.client.java.AsyncUtils.block;
import static java.util.Objects.requireNonNull;

public class UserManager {

  private final AsyncUserManager async;

  public UserManager(AsyncUserManager async) {
    this.async = requireNonNull(async);
  }

  public AsyncUserManager async() {
    return async;
  }

  public ReactiveUserManager reactive() {
    return new ReactiveUserManager(async);
  }

  public UserAndMetadata getUser(AuthDomain domain, String username) {
    return block(async.getUser(domain, username));
  }

  public UserAndMetadata getUser(AuthDomain domain, String username, GetUserOptions options) {
    return block(async.getUser(domain, username, options));
  }

  public List<RoleAndDescription> getRoles() {
    return block(async.getRoles());
  }

  public List<RoleAndDescription> getRoles(GetRolesOptions options) {
    return block(async.getRoles(options));
  }

  public List<UserAndMetadata> getAllUsers() {
    return block(async.getAllUsers());
  }

  public List<UserAndMetadata> getAllUsers(GetAllUsersOptions options) {
    return block(async.getAllUsers(options));
  }

  public void upsertUser(User user) {
    block(async.upsertUser(user));
  }

  public void upsertUser(User user, UpsertUserOptions options) {
    block(async.upsertUser(user, options));
  }

  public void dropUser(String username) {
    block(async.dropUser(username));
  }

  public void dropUser(String username, DropUserOptions options) {
    block(async.dropUser(username, options));
  }

  public Group getGroup(String name) {
    return block(async.getGroup(name));
  }

  public Group getGroup(String name, GetGroupOptions options) {
    return block(async.getGroup(name, options));
  }

  public List<Group> getAllGroups() {
    return block(async.getAllGroups());
  }

  public List<Group> getAllGroups(GetAllGroupsOptions options) {
    return block(async.getAllGroups(options));
  }

  public void upsertGroup(Group group) {
    block(async.upsertGroup(group));
  }

  public void upsertGroup(Group group, UpsertGroupOptions options) {
    block(async.upsertGroup(group, options));
  }

  public void dropGroup(String name) {
    block(async.dropGroup(name));
  }

  public void dropGroup(String name, DropGroupOptions options) {
    block(async.dropGroup(name, options));
  }
}
