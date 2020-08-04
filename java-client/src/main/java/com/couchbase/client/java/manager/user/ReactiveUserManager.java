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

import com.couchbase.client.core.Reactor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static java.util.Objects.requireNonNull;

public class ReactiveUserManager {

  private final AsyncUserManager async;

  public ReactiveUserManager(AsyncUserManager async) {
    this.async = requireNonNull(async);
  }

  public Mono<UserAndMetadata> getUser(AuthDomain domain, String username) {
    return Reactor.toMono(() -> async.getUser(domain, username));
  }

  public Mono<UserAndMetadata> getUser(AuthDomain domain, String username, GetUserOptions options) {
    return Reactor.toMono(() -> async.getUser(domain, username, options));
  }

  public Flux<UserAndMetadata> getAllUsers() {
    return Reactor.toFlux(() -> async.getAllUsers());
  }

  public Flux<UserAndMetadata> getAllUsers(GetAllUsersOptions options) {
    return Reactor.toFlux(() -> async.getAllUsers(options));
  }

  public Flux<RoleAndDescription> getRoles() {
    return Reactor.toFlux(() -> async.getRoles());
  }

  public Flux<RoleAndDescription> getRoles(GetRolesOptions options) {
    return Reactor.toFlux(() -> async.getRoles(options));
  }

  public Mono<Void> upsertUser(User user) {
    return Reactor.toMono(() -> async.upsertUser(user));
  }

  public Mono<Void> upsertUser(User user, UpsertUserOptions options) {
    return Reactor.toMono(() -> async.upsertUser(user, options));
  }

  public Mono<Void> dropUser(String username) {
    return Reactor.toMono(() -> async.dropUser(username));
  }

  public Mono<Void> dropUser(String username, DropUserOptions options) {
    return Reactor.toMono(() -> async.dropUser(username, options));
  }

  public Mono<Group> getGroup(String groupName) {
    return Reactor.toMono(() -> async.getGroup(groupName));
  }

  public Mono<Group> getGroup(String groupName, GetGroupOptions options) {
    return Reactor.toMono(() -> async.getGroup(groupName, options));
  }

  public Flux<Group> getAllGroups() {
    return Reactor.toFlux(() -> async.getAllGroups());
  }

  public Flux<Group> getAllGroups(GetAllGroupsOptions options) {
    return Reactor.toFlux(() -> async.getAllGroups(options));
  }

  public Mono<Void> upsertGroup(Group group) {
    return Reactor.toMono(() -> async.upsertGroup(group));
  }

  public Mono<Void> upsertGroup(Group group, UpsertGroupOptions options) {
    return Reactor.toMono(() -> async.upsertGroup(group, options));
  }

  public Mono<Void> dropGroup(String groupName) {
    return Reactor.toMono(() -> async.dropGroup(groupName));
  }

  public Mono<Void> dropGroup(String groupName, DropGroupOptions options) {
    return Reactor.toMono(() -> async.dropGroup(groupName, options));
  }
}
