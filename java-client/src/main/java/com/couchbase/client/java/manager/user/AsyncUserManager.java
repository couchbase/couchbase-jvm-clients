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
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.core.error.GroupNotFoundException;
import com.couchbase.client.core.error.UserNotFoundException;
import com.couchbase.client.core.json.Mapper;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.util.UrlQueryStringBuilder;
import com.couchbase.client.java.manager.ManagerSupport;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.DELETE;
import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.GET;
import static com.couchbase.client.core.deps.io.netty.handler.codec.http.HttpMethod.PUT;
import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;
import static com.couchbase.client.java.manager.user.DropGroupOptions.dropGroupOptions;
import static com.couchbase.client.java.manager.user.DropUserOptions.dropUserOptions;
import static com.couchbase.client.java.manager.user.GetAllGroupsOptions.getAllGroupsOptions;
import static com.couchbase.client.java.manager.user.GetAllUsersOptions.getAllUsersOptions;
import static com.couchbase.client.java.manager.user.GetGroupOptions.getGroupOptions;
import static com.couchbase.client.java.manager.user.GetRolesOptions.getRolesOptions;
import static com.couchbase.client.java.manager.user.GetUserOptions.getUserOptions;
import static com.couchbase.client.java.manager.user.UpsertGroupOptions.upsertGroupOptions;
import static com.couchbase.client.java.manager.user.UpsertUserOptions.upsertUserOptions;

public class AsyncUserManager extends ManagerSupport {
  // https://docs.couchbase.com/server/5.5/rest-api/rbac.html

  public AsyncUserManager(Core core) {
    super(core);
  }

  private static String pathForUsers() {
    return "/settings/rbac/users";
  }

  private static String pathForRoles() {
    return "/settings/rbac/roles";
  }

  private static String pathForUser(AuthDomain domain, String username) {
    return pathForUsers() + "/" + urlEncode(domain.alias()) + "/" + urlEncode(username);
  }

  private static String pathForGroups() {
    return "/settings/rbac/groups";
  }

  private static String pathForGroup(String name) {
    return pathForGroups() + "/" + urlEncode(name);
  }

  public CompletableFuture<UserAndMetadata> getUser(AuthDomain domain, String username) {
    return getUser(domain, username, getUserOptions());
  }

  public CompletableFuture<UserAndMetadata> getUser(AuthDomain domain, String username, GetUserOptions options) {
    GetUserOptions.Built built = options.build();
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MU_GET_USER, built.parentSpan().orElse(null));

    return sendRequest(GET, pathForUser(domain, username), built, span).thenApply(response -> {
      if (response.status() == ResponseStatus.NOT_FOUND) {
        throw UserNotFoundException.forUser(domain.alias(), username);
      }
      checkStatus(response, "get " + domain + " user [" + redactUser(username) + "]", username);
      return Mapper.decodeInto(response.content(), UserAndMetadata.class);
    });
  }

  public CompletableFuture<List<UserAndMetadata>> getAllUsers() {
    return getAllUsers(getAllUsersOptions());
  }

  public CompletableFuture<List<UserAndMetadata>> getAllUsers(GetAllUsersOptions options) {
    GetAllUsersOptions.Built built = options.build();
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MU_GET_ALL_USERS, built.parentSpan().orElse(null));

    return sendRequest(GET, pathForUsers(), built, span).thenApply(response -> {
      checkStatus(response, "get all users", null);
      return Mapper.decodeInto(response.content(), new TypeReference<List<UserAndMetadata>>() {
      });
    });
  }

  public CompletableFuture<List<RoleAndDescription>> getRoles() {
    return getRoles(getRolesOptions());
  }

  public CompletableFuture<List<RoleAndDescription>> getRoles(GetRolesOptions options) {
    GetRolesOptions.Built built = options.build();
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MU_GET_ROLES, built.parentSpan().orElse(null));

    return sendRequest(GET, pathForRoles(), built, span).thenApply(response -> {
      checkStatus(response, "get all roles", null);
      return Mapper.decodeInto(response.content(), new TypeReference<List<RoleAndDescription>>() {
      });
    });
  }

  public CompletableFuture<Void> upsertUser(User user) {
    return upsertUser(user, upsertUserOptions());
  }

  public CompletableFuture<Void> upsertUser(User user, UpsertUserOptions options) {
    UpsertUserOptions.Built built = options.build();
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MU_UPSERT_USER, built.parentSpan().orElse(null));

    final String username = user.username();

    final UrlQueryStringBuilder params = UrlQueryStringBuilder.createForUrlSafeNames()
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

    return sendRequest(PUT, pathForUser(AuthDomain.LOCAL, username), params, built, span).thenApply(response -> {
      checkStatus(response, "create user [" + redactUser(username) + "]", username);
      return null;
    });
  }

  public CompletableFuture<Void> dropUser(String username) {
    return dropUser(username, dropUserOptions());
  }

  public CompletableFuture<Void> dropUser(String username, DropUserOptions options) {
    DropUserOptions.Built built = options.build();
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MU_DROP_USER, built.parentSpan().orElse(null));

    final AuthDomain domain = AuthDomain.LOCAL;

    return sendRequest(DELETE, pathForUser(domain, username), built, span).thenApply(response -> {
      if (response.status() == ResponseStatus.NOT_FOUND) {
        throw UserNotFoundException.forUser(domain.alias(), username);
      }
      checkStatus(response, "drop user [" + redactUser(username) + "]", username);
      return null;
    });
  }

  public CompletableFuture<Group> getGroup(String groupName) {
    return getGroup(groupName, getGroupOptions());
  }

  public CompletableFuture<Group> getGroup(String groupName, GetGroupOptions options) {
    GetGroupOptions.Built built = options.build();
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MU_GET_GROUP, built.parentSpan().orElse(null));

    return sendRequest(GET, pathForGroup(groupName), built, span).thenApply(response -> {
      if (response.status() == ResponseStatus.NOT_FOUND) {
        throw GroupNotFoundException.forGroup(groupName);
      }
      checkStatus(response, "get group [" + redactMeta(groupName) + "]", groupName);
      return Mapper.decodeInto(response.content(), Group.class);
    });
  }

  public CompletableFuture<List<Group>> getAllGroups() {
    return getAllGroups(getAllGroupsOptions());
  }

  public CompletableFuture<List<Group>> getAllGroups(GetAllGroupsOptions options) {
    GetAllGroupsOptions.Built built = options.build();
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MU_GET_ALL_GROUPS, built.parentSpan().orElse(null));

    return sendRequest(GET, pathForGroups(), built, span).thenApply(response -> {
      checkStatus(response, "get all groups", null);
      return Mapper.decodeInto(response.content(), new TypeReference<List<Group>>() {
      });
    });
  }

  public CompletableFuture<Void> upsertGroup(Group group) {
    return upsertGroup(group, upsertGroupOptions());
  }

  public CompletableFuture<Void> upsertGroup(Group group, UpsertGroupOptions options) {
    UpsertGroupOptions.Built built = options.build();
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MU_UPSERT_GROUP, built.parentSpan().orElse(null));

    final UrlQueryStringBuilder params = UrlQueryStringBuilder.createForUrlSafeNames()
        .add("description", group.description())
        .add("ldap_group_ref", group.ldapGroupReference().orElse(""))
        .add("roles", group.roles().stream()
            .map(Role::format)
            .collect(Collectors.joining(",")));

    return sendRequest(PUT, pathForGroup(group.name()), params, built, span).thenApply(response -> {
      checkStatus(response, "create group [" + redactMeta(group.name()) + "]", group.name());
      return null;
    });
  }

  public CompletableFuture<Void> dropGroup(String groupName) {
    return dropGroup(groupName, dropGroupOptions());
  }

  public CompletableFuture<Void> dropGroup(String groupName, DropGroupOptions options) {
    DropGroupOptions.Built built = options.build();
    RequestSpan span = buildSpan(TracingIdentifiers.SPAN_REQUEST_MU_DROP_GROUP, built.parentSpan().orElse(null));

    return sendRequest(DELETE, pathForGroup(groupName), built, span).thenApply(response -> {
      if (response.status() == ResponseStatus.NOT_FOUND) {
        throw GroupNotFoundException.forGroup(groupName);
      }
      checkStatus(response, "drop group [" + redactMeta(groupName) + "]", groupName);
      return null;
    });
  }

  private RequestSpan buildSpan(final String spanName, final RequestSpan parent) {
    RequestSpan span = environment().requestTracer().requestSpan(spanName, parent);
    span.attribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);
    return span;
  }

}
