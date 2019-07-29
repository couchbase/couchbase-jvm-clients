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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.type.TypeReference;
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
import static com.couchbase.client.core.logging.RedactableArgument.redactUser;
import static com.couchbase.client.core.util.UrlQueryStringBuilder.urlEncode;
import static com.couchbase.client.java.manager.user.UpsertUserOptions.upsertUserOptions;

@Stability.Volatile
public class AsyncUserManager extends ManagerSupport {
  // https://docs.couchbase.com/server/5.5/rest-api/rbac.html

  private final AsyncGroupManager groups;

  public AsyncUserManager(Core core) {
    super(core);
    groups = new AsyncGroupManager(core);
  }

  public AsyncGroupManager groups() {
    return groups;
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

  public CompletableFuture<UserAndMetadata> get(AuthDomain domain, String username) {
    return sendRequest(GET, pathForUser(domain, username)).thenApply(response -> {
      if (response.status() == ResponseStatus.NOT_FOUND) {
        throw UserNotFoundException.forUser(domain, username);
      }
      checkStatus(response, "get " + domain + " user [" + redactUser(username) + "]");
      return Mapper.decodeInto(response.content(), UserAndMetadata.class);
    });
  }

  public CompletableFuture<List<UserAndMetadata>> getAll() {
    return sendRequest(GET, pathForUsers()).thenApply(response -> {
      checkStatus(response, "get all users");
      return Mapper.decodeInto(response.content(), new TypeReference<List<UserAndMetadata>>() {
      });
    });
  }

  public CompletableFuture<List<RoleAndDescription>> availableRoles() {
    return sendRequest(GET, pathForRoles()).thenApply(response -> {
      checkStatus(response, "get all roles");
      return Mapper.decodeInto(response.content(), new TypeReference<List<RoleAndDescription>>() {
      });
    });
  }

  public CompletableFuture<Void> upsert(User user) {
    return upsert(user, upsertUserOptions());
  }

  public CompletableFuture<Void> upsert(User user, UpsertUserOptions options) {
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

    return sendRequest(PUT, pathForUser(AuthDomain.LOCAL, username), params).thenApply(response -> {
      checkStatus(response, "create user [" + redactUser(username) + "]");
      return null;
    });
  }

  public CompletableFuture<Void> drop(String username) {
    final AuthDomain domain = AuthDomain.LOCAL;

    return sendRequest(DELETE, pathForUser(domain, username)).thenApply(response -> {
      if (response.status() == ResponseStatus.NOT_FOUND) {
        throw UserNotFoundException.forUser(domain, username);
      }
      checkStatus(response, "drop user [" + redactUser(username) + "]");
      return null;
    });
  }
}
