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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.endpoint.http.CoreHttpClient;
import com.couchbase.client.core.endpoint.http.CoreHttpPath;
import com.couchbase.client.core.endpoint.http.CoreHttpResponse;
import com.couchbase.client.core.error.GroupNotFoundException;
import com.couchbase.client.core.error.UserNotFoundException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.RequestTarget;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.protostellar.CoreProtostellarUtil;
import com.couchbase.client.core.util.UrlQueryStringBuilder;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.couchbase.client.core.endpoint.http.CoreHttpPath.path;
import static com.couchbase.client.core.endpoint.http.CoreHttpRequest.Builder.newForm;
import static com.couchbase.client.core.error.HttpStatusCodeException.couchbaseResponseStatus;
import static com.couchbase.client.core.util.CbCollections.mapOf;
import static com.couchbase.client.core.util.CbThrowables.propagate;
import static com.couchbase.client.java.manager.user.ChangePasswordOptions.changePasswordOptions;
import static com.couchbase.client.java.manager.user.DropGroupOptions.dropGroupOptions;
import static com.couchbase.client.java.manager.user.DropUserOptions.dropUserOptions;
import static com.couchbase.client.java.manager.user.GetAllGroupsOptions.getAllGroupsOptions;
import static com.couchbase.client.java.manager.user.GetAllUsersOptions.getAllUsersOptions;
import static com.couchbase.client.java.manager.user.GetGroupOptions.getGroupOptions;
import static com.couchbase.client.java.manager.user.GetRolesOptions.getRolesOptions;
import static com.couchbase.client.java.manager.user.GetUserOptions.getUserOptions;
import static com.couchbase.client.java.manager.user.UpsertGroupOptions.upsertGroupOptions;
import static com.couchbase.client.java.manager.user.UpsertUserOptions.upsertUserOptions;

public class AsyncUserManager {
  // https://docs.couchbase.com/server/5.5/rest-api/rbac.html

  private final Core core;
  private final CoreHttpClient httpClient;

  public AsyncUserManager(Core core) {
    this.core = core;
    this.httpClient = core.httpClient(RequestTarget.manager());
  }

  private static CoreHttpPath pathForUsers() {
    return path("/settings/rbac/users");
  }

  private static CoreHttpPath pathForRoles() {
    return path("/settings/rbac/roles");
  }

  private static CoreHttpPath pathForUser(AuthDomain domain, String username) {
    return path("/settings/rbac/users/{domain}/{username}", mapOf(
        "username", username,
        "domain", domain.alias()
    ));
  }

  private static CoreHttpPath pathForGroups() {
    return path("/settings/rbac/groups");
  }

  private static CoreHttpPath pathForGroup(String groupName) {
    return path("/settings/rbac/groups/{groupName}", mapOf(
        "groupName", groupName
    ));
  }
  private static CoreHttpPath pathForPassword() {
    return path("/controller/changePassword");
  }

  public CompletableFuture<UserAndMetadata> getUser(AuthDomain domain, String username) {
    return getUser(domain, username, getUserOptions());
  }

  public CompletableFuture<UserAndMetadata> getUser(AuthDomain domain, String username, GetUserOptions options) {
    checkIfProtostellar();

    return httpClient.get(pathForUser(domain, username), options.build())
        .trace(TracingIdentifiers.SPAN_REQUEST_MU_GET_USER)
        .exec(core)
        .exceptionally(translateNotFound(() -> UserNotFoundException.forUser(domain.alias(), username)))
        .thenApply(response -> Mapper.decodeInto(response.content(), UserAndMetadata.class));
  }

  public CompletableFuture<List<UserAndMetadata>> getAllUsers() {
    return getAllUsers(getAllUsersOptions());
  }

  public CompletableFuture<List<UserAndMetadata>> getAllUsers(GetAllUsersOptions options) {
    checkIfProtostellar();

    return httpClient.get(pathForUsers(), options.build())
        .trace(TracingIdentifiers.SPAN_REQUEST_MU_GET_ALL_USERS)
        .exec(core)
        .thenApply(response -> Mapper.decodeInto(response.content(), new TypeReference<List<UserAndMetadata>>() {
        }));
  }

  public CompletableFuture<List<RoleAndDescription>> getRoles() {
    return getRoles(getRolesOptions());
  }

  public CompletableFuture<List<RoleAndDescription>> getRoles(GetRolesOptions options) {
    checkIfProtostellar();

    return httpClient.get(pathForRoles(), options.build())
        .trace(TracingIdentifiers.SPAN_REQUEST_MU_GET_ROLES)
        .exec(core)
        .thenApply(response -> Mapper.decodeInto(response.content(), new TypeReference<List<RoleAndDescription>>() {
        }));
  }

  /**
   * Changes the password of the currently authenticated user.
   * SDK must be re-started and a new connection established after running, as the previous credentials will no longer
   * be valid.
   * @param newPassword String to replace the previous password with.
   */
  public CompletableFuture<Void> changePassword(String newPassword){
    return changePassword(newPassword, changePasswordOptions());

  }
  /**
   * Changes the password of the currently authenticated user.
   * SDK must be re-started and a new connection established after running, as the previous credentials will no longer
   * be valid.
   * @param newPassword String to replace the previous password with.
   * @param options Common options (timeout, retry...)
   */
  public CompletableFuture<Void> changePassword(String newPassword, ChangePasswordOptions options){
    checkIfProtostellar();

    UrlQueryStringBuilder params = newForm()
            .add("password", newPassword);

    return httpClient.post(pathForPassword(), options.build())
            .trace(TracingIdentifiers.SPAN_REQUEST_MU_CHANGE_PASSWORD)
            .form(params)
            .exec(core)
            .thenApply(response -> null);

  }

  public CompletableFuture<Void> upsertUser(User user) {
    return upsertUser(user, upsertUserOptions());
  }

  public CompletableFuture<Void> upsertUser(User user, UpsertUserOptions options) {
    checkIfProtostellar();

    String username = user.username();
    UrlQueryStringBuilder params = newForm()
        .add("name", user.displayName())
        .add("roles", user.roles().stream()
            .map(Role::format)
            .collect(Collectors.joining(",")));

    // Omit empty group list for compatibility with Couchbase Server versions < 6.5.
    // Versions >= 6.5 treat the absent parameter just like an empty list.
    if (!user.groups().isEmpty()) {
      params.add("groups", String.join(",", user.groups()));
    }

    // Password is required when creating user, but optional when updating existing user.
    user.password().ifPresent(pwd -> params.add("password", pwd));

    return httpClient.put(pathForUser(AuthDomain.LOCAL, username), options.build())
        .trace(TracingIdentifiers.SPAN_REQUEST_MU_UPSERT_USER)
        .form(params)
        .exec(core)
        .thenApply(response -> null);
  }

  public CompletableFuture<Void> dropUser(String username) {
    return dropUser(username, dropUserOptions());
  }

  public CompletableFuture<Void> dropUser(String username, DropUserOptions options) {
    checkIfProtostellar();

    AuthDomain domain = AuthDomain.LOCAL;

    return httpClient.delete(pathForUser(domain, username), options.build())
        .trace(TracingIdentifiers.SPAN_REQUEST_MU_DROP_USER)
        .exec(core)
        .exceptionally(translateNotFound(() -> UserNotFoundException.forUser(domain.alias(), username)))
        .thenApply(response -> null);
  }

  public CompletableFuture<Group> getGroup(String groupName) {
    return getGroup(groupName, getGroupOptions());
  }

  public CompletableFuture<Group> getGroup(String groupName, GetGroupOptions options) {
    checkIfProtostellar();

    return httpClient.get(pathForGroup(groupName), options.build())
        .trace(TracingIdentifiers.SPAN_REQUEST_MU_GET_GROUP)
        .exec(core)
        .exceptionally(translateNotFound(() -> GroupNotFoundException.forGroup(groupName)))
        .thenApply(response -> Mapper.decodeInto(response.content(), Group.class));
  }

  public CompletableFuture<List<Group>> getAllGroups() {
    return getAllGroups(getAllGroupsOptions());
  }

  public CompletableFuture<List<Group>> getAllGroups(GetAllGroupsOptions options) {
    checkIfProtostellar();

    return httpClient.get(pathForGroups(), options.build())
        .trace(TracingIdentifiers.SPAN_REQUEST_MU_GET_ALL_GROUPS)
        .exec(core)
        .thenApply(response -> Mapper.decodeInto(response.content(), new TypeReference<List<Group>>() {
        }));
  }

  public CompletableFuture<Void> upsertGroup(Group group) {
    return upsertGroup(group, upsertGroupOptions());
  }

  public CompletableFuture<Void> upsertGroup(Group group, UpsertGroupOptions options) {
    checkIfProtostellar();

    UrlQueryStringBuilder params = newForm()
        .add("description", group.description())
        .add("ldap_group_ref", group.ldapGroupReference().orElse(""))
        .add("roles", group.roles().stream()
            .map(Role::format)
            .collect(Collectors.joining(",")));

    return httpClient.put(pathForGroup(group.name()), options.build())
        .trace(TracingIdentifiers.SPAN_REQUEST_MU_UPSERT_GROUP)
        .form(params)
        .exec(core)
        .thenApply(response -> null);
  }

  public CompletableFuture<Void> dropGroup(String groupName) {
    return dropGroup(groupName, dropGroupOptions());
  }

  public CompletableFuture<Void> dropGroup(String groupName, DropGroupOptions options) {
    return httpClient.delete(pathForGroup(groupName), options.build())
        .trace(TracingIdentifiers.SPAN_REQUEST_MU_DROP_GROUP)
        .exec(core)
        .exceptionally(translateNotFound(() -> GroupNotFoundException.forGroup(groupName)))
        .thenApply(response -> null);
  }

  private static Function<Throwable, CoreHttpResponse> translateNotFound(Supplier<? extends RuntimeException> exceptionSupplier) {
    return t -> {
      throw couchbaseResponseStatus(t) == ResponseStatus.NOT_FOUND
          ? exceptionSupplier.get()
          : propagate(t);
    };
  }

  private void checkIfProtostellar() {
    if (core.isProtostellar()) {
      throw CoreProtostellarUtil.unsupportedInProtostellar("user management");
    }
  }
}
