/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.manager;

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CommonOptions;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.manager.user.AuthDomain;
import com.couchbase.client.java.manager.user.ChangePasswordOptions;
import com.couchbase.client.java.manager.user.DropGroupOptions;
import com.couchbase.client.java.manager.user.DropUserOptions;
import com.couchbase.client.java.manager.user.GetAllGroupsOptions;
import com.couchbase.client.java.manager.user.GetAllUsersOptions;
import com.couchbase.client.java.manager.user.GetGroupOptions;
import com.couchbase.client.java.manager.user.GetRolesOptions;
import com.couchbase.client.java.manager.user.GetUserOptions;
import com.couchbase.client.java.manager.user.Group;
import com.couchbase.client.java.manager.user.ReactiveUserManager;
import com.couchbase.client.java.manager.user.Role;
import com.couchbase.client.java.manager.user.RoleAndDescription;
import com.couchbase.client.java.manager.user.RoleAndOrigins;
import com.couchbase.client.java.manager.user.UpsertGroupOptions;
import com.couchbase.client.java.manager.user.UpsertUserOptions;
import com.couchbase.client.java.manager.user.User;
import com.couchbase.client.java.manager.user.UserAndMetadata;
import com.couchbase.client.java.manager.user.UserManager;
import com.couchbase.client.protocol.run.Result;
import com.couchbase.client.protocol.sdk.cluster.usermanager.Command;
import com.couchbase.client.protocol.sdk.cluster.usermanager.GetAllGroupsResult;
import com.couchbase.client.protocol.sdk.cluster.usermanager.GetAllUsersResult;
import com.couchbase.client.protocol.sdk.cluster.usermanager.GetGroupResult;
import com.couchbase.client.protocol.sdk.cluster.usermanager.GetRolesResult;
import com.couchbase.client.protocol.sdk.cluster.usermanager.GetUserResult;
import com.couchbase.client.protocol.sdk.cluster.usermanager.Origin;
import com.google.protobuf.Timestamp;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.couchbase.client.performer.core.util.TimeUtil.getTimeNow;
import static com.couchbase.utils.OptionsUtil.convertDuration;
import static com.couchbase.utils.UserSchedulerUtil.withSchedulerCheck;

/**
 * The Java SDK's API shape differs from the RFC in AuthDomain:
 * <p>
 * This is relayed to the driver through the presence of 
 * SDK_USER_MANAGER_GET_ALL_USERS_RETURNS_ALL_DOMAINS and lack of SDK_USER_MANAGER_EXTERNAL_DOMAIN caps.
 * <ul>
 *   <li>getUser, upsertUser and dropUser only operate on LOCAL users, so specifying EXTERNAL is unsupported.
 *   (getUser takes the domain as a required parameter, so LOCAL is always passed.)</li>
 *   <li>getAllUsers has no domain parameter and always returns users from all domains, so any domain is ignored.</li>
 * </ul>
 */
public class UserManagerHelper {

  private UserManagerHelper() {
  }

  public static void handleUserManager(Cluster cluster,
                                       ConcurrentHashMap<String, RequestSpan> spans,
                                       com.couchbase.client.protocol.sdk.Command command,
                                       Result.Builder result) {
    var um = command.getClusterCommand().getUserManager();
    handle(Ops.blocking(cluster.users()), um, spans, result).block();
  }

  public static Mono<Result> handleUserManagerReactive(ReactiveCluster cluster,
                                                       ConcurrentHashMap<String, RequestSpan> spans,
                                                       com.couchbase.client.protocol.sdk.Command command,
                                                       Result.Builder result) {
    var um = command.getClusterCommand().getUserManager();
    return handle(Ops.reactive(cluster.users()), um, spans, result);
  }

  private static Mono<Result> handle(Ops ops,
                                     Command um,
                                     ConcurrentHashMap<String, RequestSpan> spans,
                                     Result.Builder result) {
    return Mono.defer(() -> {
      result.setInitiated(getTimeNow());
      long start = System.nanoTime();

      Mono<com.couchbase.client.protocol.sdk.Result> response = switch (um.getCommandCase()) {
        case GET_USER -> {
          var req = um.getGetUser();
          GetUserOptions options = null;
          if (req.hasOptions()) {
            var o = req.getOptions();
            if (o.hasDomainName()) requireLocalDomain(o.getDomainName(), "getUser");
            options = options(GetUserOptions.getUserOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          }
          // The Java SDK requires a domain parameter for getUser, and only LOCAL is supported (as with upsertUser and dropUser).
          yield ops.getUser().apply(new GetUserArgs(AuthDomain.LOCAL, req.getUsername()), options)
                  .map(u -> userManagerResult(com.couchbase.client.protocol.sdk.cluster.usermanager.Result.newBuilder()
                          .setGetUserResult(GetUserResult.newBuilder().setUser(toFit(u)))));
        }
        case GET_ALL_USERS -> {
          var req = um.getGetAllUsers();
          GetAllUsersOptions options = null;
          if (req.hasOptions()) {
            var o = req.getOptions();
            // The Java SDK has no domain option for getAllUsers (it returns users from all domains), so domain_name is ignored.
            options = options(GetAllUsersOptions.getAllUsersOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          }
          yield ops.getAllUsers().apply(options)
                  .map(users -> userManagerResult(com.couchbase.client.protocol.sdk.cluster.usermanager.Result.newBuilder()
                          .setGetAllUsersResult(GetAllUsersResult.newBuilder()
                                  .addAllUsers(users.stream().map(UserManagerHelper::toFit).toList()))));
        }
        case UPSERT_USER -> {
          var req = um.getUpsertUser();
          UpsertUserOptions options = null;
          if (req.hasOptions()) {
            var o = req.getOptions();
            if (o.hasDomainName()) requireLocalDomain(o.getDomainName(), "upsertUser");
            options = options(UpsertUserOptions.upsertUserOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          }
          yield ops.upsertUser().apply(toSdk(req.getUser()), options).then(Mono.fromSupplier(UserManagerHelper::success));
        }
        case DROP_USER -> {
          var req = um.getDropUser();
          DropUserOptions options = null;
          if (req.hasOptions()) {
            var o = req.getOptions();
            if (o.hasDomainName()) requireLocalDomain(o.getDomainName(), "dropUser");
            options = options(DropUserOptions.dropUserOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          }
          yield ops.dropUser().apply(req.getUsername(), options).then(Mono.fromSupplier(UserManagerHelper::success));
        }
        case GET_ROLES -> {
          var req = um.getGetRoles();
          GetRolesOptions options = null;
          if (req.hasOptions()) {
            var o = req.getOptions();
            options = options(GetRolesOptions.getRolesOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          }
          yield ops.getRoles().apply(options)
                  .map(roles -> userManagerResult(com.couchbase.client.protocol.sdk.cluster.usermanager.Result.newBuilder()
                          .setGetRolesResult(GetRolesResult.newBuilder()
                                  .addAllRoles(roles.stream().map(UserManagerHelper::toFit).toList()))));
        }
        case GET_GROUP -> {
          var req = um.getGetGroup();
          GetGroupOptions options = null;
          if (req.hasOptions()) {
            var o = req.getOptions();
            options = options(GetGroupOptions.getGroupOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          }
          yield ops.getGroup().apply(req.getGroupName(), options)
                  .map(g -> userManagerResult(com.couchbase.client.protocol.sdk.cluster.usermanager.Result.newBuilder()
                          .setGetGroupResult(GetGroupResult.newBuilder().setGroup(toFit(g)))));
        }
        case GET_ALL_GROUPS -> {
          var req = um.getGetAllGroups();
          GetAllGroupsOptions options = null;
          if (req.hasOptions()) {
            var o = req.getOptions();
            options = options(GetAllGroupsOptions.getAllGroupsOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          }
          yield ops.getAllGroups().apply(options)
                  .map(groups -> userManagerResult(com.couchbase.client.protocol.sdk.cluster.usermanager.Result.newBuilder()
                          .setGetAllGroupsResult(GetAllGroupsResult.newBuilder()
                                  .addAllGroups(groups.stream().map(UserManagerHelper::toFit).toList()))));
        }
        case UPSERT_GROUP -> {
          var req = um.getUpsertGroup();
          UpsertGroupOptions options = null;
          if (req.hasOptions()) {
            var o = req.getOptions();
            options = options(UpsertGroupOptions.upsertGroupOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          }
          yield ops.upsertGroup().apply(toSdk(req.getGroup()), options).then(Mono.fromSupplier(UserManagerHelper::success));
        }
        case DROP_GROUP -> {
          var req = um.getDropGroup();
          DropGroupOptions options = null;
          if (req.hasOptions()) {
            var o = req.getOptions();
            options = options(DropGroupOptions.dropGroupOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          }
          yield ops.dropGroup().apply(req.getGroupName(), options).then(Mono.fromSupplier(UserManagerHelper::success));
        }
        case CHANGE_PASSWORD -> {
          var req = um.getChangePassword();
          ChangePasswordOptions options = null;
          if (req.hasOptions()) {
            var o = req.getOptions();
            options = options(ChangePasswordOptions.changePasswordOptions(), o.hasTimeout(), o.getTimeout(), o.hasParentSpanId(), o.getParentSpanId(), spans);
          }
          yield ops.changePassword().apply(req.getNewPassword(), options).then(Mono.fromSupplier(UserManagerHelper::success));
        }
        default -> Mono.error(new UnsupportedOperationException("Unknown user manager command " + um.getCommandCase()));
      };

      return response.map(r -> {
        result.setElapsedNanos(System.nanoTime() - start);
        result.setSdk(r);
        return result.build();
      });
    });
  }

  private static void requireLocalDomain(com.couchbase.client.protocol.sdk.cluster.usermanager.AuthDomain domain, String op) {
    if (domain != com.couchbase.client.protocol.sdk.cluster.usermanager.AuthDomain.LOCAL) {
      throw new UnsupportedOperationException("The Java SDK's " + op + " only supports the LOCAL domain, but got " + domain);
    }
  }

  private static <T extends CommonOptions<T>> T options(T options,
                                                        boolean hasTimeout,
                                                        com.google.protobuf.Duration timeout,
                                                        boolean hasParentSpanId,
                                                        String parentSpanId,
                                                        ConcurrentHashMap<String, RequestSpan> spans) {
    if (hasTimeout) options.timeout(convertDuration(timeout));
    if (hasParentSpanId) options.parentSpan(spans.get(parentSpanId));
    return options;
  }

  private static com.couchbase.client.protocol.sdk.Result success() {
    return com.couchbase.client.protocol.sdk.Result.newBuilder().setSuccess(true).build();
  }

  private static com.couchbase.client.protocol.sdk.Result userManagerResult(com.couchbase.client.protocol.sdk.cluster.usermanager.Result.Builder result) {
    return com.couchbase.client.protocol.sdk.Result.newBuilder().setUserManagerResult(result).build();
  }

  // FIT -> SDK

  private static Role toSdk(com.couchbase.client.protocol.sdk.cluster.usermanager.Role in) {
    return new Role(
            in.getName(),
            in.hasBucket() ? in.getBucket() : null,
            in.hasScope() ? in.getScope() : null,
            in.hasCollection() ? in.getCollection() : null);
  }

  private static User toSdk(com.couchbase.client.protocol.sdk.cluster.usermanager.User in) {
    var out = new User(in.getUsername())
            .displayName(in.getDisplayName())
            .groups(in.getGroupsList())
            .roles(in.getRolesList().stream().map(UserManagerHelper::toSdk).toList());
    if (in.hasPassword()) out.password(in.getPassword());
    return out;
  }

  private static Group toSdk(com.couchbase.client.protocol.sdk.cluster.usermanager.Group in) {
    var out = new Group(in.getName())
            .roles(in.getRolesList().stream().map(UserManagerHelper::toSdk).toList());
    if (in.hasDescription()) out.description(in.getDescription());
    if (in.hasLdapGroupReference()) out.ldapGroupReference(in.getLdapGroupReference());
    return out;
  }

  // SDK -> FIT

  private static com.couchbase.client.protocol.sdk.cluster.usermanager.AuthDomain toFit(AuthDomain in) {
    return switch (in) {
      case LOCAL -> com.couchbase.client.protocol.sdk.cluster.usermanager.AuthDomain.LOCAL;
      case EXTERNAL -> com.couchbase.client.protocol.sdk.cluster.usermanager.AuthDomain.EXTERNAL;
    };
  }

  private static com.couchbase.client.protocol.sdk.cluster.usermanager.Role toFit(Role in) {
    var out = com.couchbase.client.protocol.sdk.cluster.usermanager.Role.newBuilder()
            .setName(in.name());
    in.bucket().ifPresent(out::setBucket);
    in.scope().ifPresent(out::setScope);
    in.collection().ifPresent(out::setCollection);
    return out.build();
  }

  private static com.couchbase.client.protocol.sdk.cluster.usermanager.RoleAndDescription toFit(RoleAndDescription in) {
    var out = com.couchbase.client.protocol.sdk.cluster.usermanager.RoleAndDescription.newBuilder()
            .setRole(toFit(in.role()));
    if (in.displayName() != null) out.setDisplayName(in.displayName());
    if (in.description() != null) out.setDescription(in.description());
    return out.build();
  }

  private static com.couchbase.client.protocol.sdk.cluster.usermanager.RoleAndOrigins toFit(RoleAndOrigins in) {
    return com.couchbase.client.protocol.sdk.cluster.usermanager.RoleAndOrigins.newBuilder()
            .setRole(toFit(in.role()))
            .addAllOrigins(in.origins().stream().map(UserManagerHelper::toFit).toList())
            .build();
  }

  private static Origin toFit(RoleAndOrigins.Origin in) {
    var out = Origin.newBuilder().setType(in.type());
    in.name().ifPresent(out::setName);
    return out.build();
  }

  private static com.couchbase.client.protocol.sdk.cluster.usermanager.User toFit(User in) {
    // Password is write-only, and never returned.
    return com.couchbase.client.protocol.sdk.cluster.usermanager.User.newBuilder()
            .setUsername(in.username())
            .setDisplayName(in.displayName())
            .addAllGroups(in.groups())
            .addAllRoles(in.roles().stream().map(UserManagerHelper::toFit).toList())
            .build();
  }

  private static com.couchbase.client.protocol.sdk.cluster.usermanager.UserAndMetadata toFit(UserAndMetadata in) {
    var out = com.couchbase.client.protocol.sdk.cluster.usermanager.UserAndMetadata.newBuilder()
            .setDomain(toFit(in.domain()))
            .setUser(toFit(in.user()))
            .addAllEffectiveRoles(in.effectiveRolesAndOrigins().stream().map(UserManagerHelper::toFit).toList())
            .addAllExternalGroups(in.externalGroups());
    in.passwordChanged().ifPresent(i -> out.setPasswordChanged(Timestamp.newBuilder()
            .setSeconds(i.getEpochSecond())
            .setNanos(i.getNano())));
    return out.build();
  }

  private static com.couchbase.client.protocol.sdk.cluster.usermanager.Group toFit(Group in) {
    var out = com.couchbase.client.protocol.sdk.cluster.usermanager.Group.newBuilder()
            .setName(in.name())
            .setDescription(in.description())
            .addAllRoles(in.roles().stream().map(UserManagerHelper::toFit).toList());
    in.ldapGroupReference().ifPresent(out::setLdapGroupReference);
    return out.build();
  }

  private record GetUserArgs(AuthDomain domain, String username) {
  }

  /**
   * Abstracts over the blocking and reactive APIs.  A null options argument means the driver did not provide
   * options, and the SDK overload without options is called.
   */
  private record Ops(
          BiFunction<GetUserArgs, GetUserOptions, Mono<UserAndMetadata>> getUser,
          Function<GetAllUsersOptions, Mono<List<UserAndMetadata>>> getAllUsers,
          BiFunction<User, UpsertUserOptions, Mono<Void>> upsertUser,
          BiFunction<String, DropUserOptions, Mono<Void>> dropUser,
          Function<GetRolesOptions, Mono<List<RoleAndDescription>>> getRoles,
          BiFunction<String, GetGroupOptions, Mono<Group>> getGroup,
          Function<GetAllGroupsOptions, Mono<List<Group>>> getAllGroups,
          BiFunction<Group, UpsertGroupOptions, Mono<Void>> upsertGroup,
          BiFunction<String, DropGroupOptions, Mono<Void>> dropGroup,
          BiFunction<String, ChangePasswordOptions, Mono<Void>> changePassword) {

    static Ops blocking(UserManager m) {
      return new Ops(
              (a, o) -> Mono.fromCallable(() -> o == null ? m.getUser(a.domain(), a.username()) : m.getUser(a.domain(), a.username(), o)),
              o -> Mono.fromCallable(() -> o == null ? m.getAllUsers() : m.getAllUsers(o)),
              (u, o) -> Mono.fromRunnable(() -> { if (o == null) m.upsertUser(u); else m.upsertUser(u, o); }),
              (n, o) -> Mono.fromRunnable(() -> { if (o == null) m.dropUser(n); else m.dropUser(n, o); }),
              o -> Mono.fromCallable(() -> o == null ? m.getRoles() : m.getRoles(o)),
              (n, o) -> Mono.fromCallable(() -> o == null ? m.getGroup(n) : m.getGroup(n, o)),
              o -> Mono.fromCallable(() -> o == null ? m.getAllGroups() : m.getAllGroups(o)),
              (g, o) -> Mono.fromRunnable(() -> { if (o == null) m.upsertGroup(g); else m.upsertGroup(g, o); }),
              (n, o) -> Mono.fromRunnable(() -> { if (o == null) m.dropGroup(n); else m.dropGroup(n, o); }),
              (p, o) -> Mono.fromRunnable(() -> { if (o == null) m.changePassword(p); else m.changePassword(p, o); }));
    }

    static Ops reactive(ReactiveUserManager m) {
      return new Ops(
              (a, o) -> withSchedulerCheck(o == null ? m.getUser(a.domain(), a.username()) : m.getUser(a.domain(), a.username(), o)),
              o -> withSchedulerCheck(o == null ? m.getAllUsers() : m.getAllUsers(o)).collectList(),
              (u, o) -> withSchedulerCheck(o == null ? m.upsertUser(u) : m.upsertUser(u, o)),
              (n, o) -> withSchedulerCheck(o == null ? m.dropUser(n) : m.dropUser(n, o)),
              o -> withSchedulerCheck(o == null ? m.getRoles() : m.getRoles(o)).collectList(),
              (n, o) -> withSchedulerCheck(o == null ? m.getGroup(n) : m.getGroup(n, o)),
              o -> withSchedulerCheck(o == null ? m.getAllGroups() : m.getAllGroups(o)).collectList(),
              (g, o) -> withSchedulerCheck(o == null ? m.upsertGroup(g) : m.upsertGroup(g, o)),
              (n, o) -> withSchedulerCheck(o == null ? m.dropGroup(n) : m.dropGroup(n, o)),
              (p, o) -> withSchedulerCheck(o == null ? m.changePassword(p) : m.changePassword(p, o)));
    }
  }
}
