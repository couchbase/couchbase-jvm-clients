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

import java.util.List;

import static com.couchbase.client.java.AsyncUtils.block;
import static com.couchbase.client.java.manager.user.UpsertUserOptions.upsertUserOptions;
import static java.util.Objects.requireNonNull;

@Stability.Volatile
public class UserManager {

  private final AsyncUserManager async;
  private final GroupManager groups;

  public UserManager(AsyncUserManager async) {
    this.async = requireNonNull(async);
    this.groups = new GroupManager(async.groups());
  }

  public GroupManager groups() {
    return groups;
  }

  public UserAndMetadata get(AuthDomain domain, String username) {
    return block(async.get(domain, username));
  }

  public List<RoleAndDescription> availableRoles() {
    return block(async.availableRoles());
  }

  public List<UserAndMetadata> getAll() {
    return block(async.getAll());
  }

  public void create(User user, String userPassword) {
    block(async.create(user, userPassword));
  }

  public void upsert(User user) {
    block(async.upsert(user));
  }

  public void upsert(User user, UpsertUserOptions options) {
    block(async.upsert(user, options));
  }

  public void drop(String username) {
    block(async.drop(username));
  }
}
