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

import com.couchbase.client.core.util.ReactorOps;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.couchbase.client.core.Reactor.toFlux;
import static java.util.Objects.requireNonNull;

public class ReactiveUserManager {

  private final AsyncUserManager async;
  private final ReactorOps reactor;

  public ReactiveUserManager(ReactorOps reactor, AsyncUserManager async) {
    this.reactor = requireNonNull(reactor);
    this.async = requireNonNull(async);
  }

  public Mono<UserAndMetadata> getUser(AuthDomain domain, String username) {
    return reactor.publishOnUserScheduler(() -> async.getUser(domain, username));
  }

  public Mono<UserAndMetadata> getUser(AuthDomain domain, String username, GetUserOptions options) {
    return reactor.publishOnUserScheduler(() -> async.getUser(domain, username, options));
  }

  public Flux<UserAndMetadata> getAllUsers() {
    return reactor.publishOnUserScheduler(toFlux(() -> async.getAllUsers()));
  }

  public Flux<UserAndMetadata> getAllUsers(GetAllUsersOptions options) {
    return reactor.publishOnUserScheduler(toFlux(() -> async.getAllUsers(options)));
  }

  public Flux<RoleAndDescription> getRoles() {
    return reactor.publishOnUserScheduler(toFlux(() -> async.getRoles()));
  }

  public Flux<RoleAndDescription> getRoles(GetRolesOptions options) {
    return reactor.publishOnUserScheduler(toFlux(() -> async.getRoles(options)));
  }

  /**
   * Changes the password of the currently authenticated user.
   * SDK must be re-started and a new connection established after running, as the previous credentials will no longer
   * be valid.
   * @param newPassword String to replace the previous password with.
   * @param options Common options (timeout, retry...)
   */
  public Mono<Void> changePassword(String newPassword, ChangePasswordOptions options) {
    return reactor.publishOnUserScheduler(() -> async.changePassword(newPassword, options));
  }
  /**
   * Changes the password of the currently authenticated user.
   * SDK must be re-started and a new connection established after running, as the previous credentials will no longer
   * be valid.
   * @param newPassword String to replace the previous password with.
   */
  public Mono<Void> changePassword(String newPassword) { return reactor.publishOnUserScheduler(() -> async.changePassword(newPassword)); }

  public Mono<Void> upsertUser(User user) {
    return reactor.publishOnUserScheduler(() -> async.upsertUser(user));
  }

  public Mono<Void> upsertUser(User user, UpsertUserOptions options) {
    return reactor.publishOnUserScheduler(() -> async.upsertUser(user, options));
  }

  public Mono<Void> dropUser(String username) {
    return reactor.publishOnUserScheduler(() -> async.dropUser(username));
  }

  public Mono<Void> dropUser(String username, DropUserOptions options) {
    return reactor.publishOnUserScheduler(() -> async.dropUser(username, options));
  }

  public Mono<Group> getGroup(String groupName) {
    return reactor.publishOnUserScheduler(() -> async.getGroup(groupName));
  }

  public Mono<Group> getGroup(String groupName, GetGroupOptions options) {
    return reactor.publishOnUserScheduler(() -> async.getGroup(groupName, options));
  }

  public Flux<Group> getAllGroups() {
    return reactor.publishOnUserScheduler(toFlux(() -> async.getAllGroups()));
  }

  public Flux<Group> getAllGroups(GetAllGroupsOptions options) {
    return reactor.publishOnUserScheduler(toFlux(() -> async.getAllGroups(options)));
  }

  public Mono<Void> upsertGroup(Group group) {
    return reactor.publishOnUserScheduler(() -> async.upsertGroup(group));
  }

  public Mono<Void> upsertGroup(Group group, UpsertGroupOptions options) {
    return reactor.publishOnUserScheduler(() -> async.upsertGroup(group, options));
  }

  public Mono<Void> dropGroup(String groupName) {
    return reactor.publishOnUserScheduler(() -> async.dropGroup(groupName));
  }

  public Mono<Void> dropGroup(String groupName, DropGroupOptions options) {
    return reactor.publishOnUserScheduler(() -> async.dropGroup(groupName, options));
  }
}
