/*
 * Copyright 2022 Couchbase, Inc.
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
package com.couchbase.client.java.util;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.error.GroupNotFoundException;
import com.couchbase.client.core.error.UserNotFoundException;
import com.couchbase.client.core.util.ConsistencyUtil;
import com.couchbase.client.java.manager.user.UserManager;

import static com.couchbase.client.java.manager.user.AuthDomain.LOCAL;

/**
 * DRYs some test logic related to users and groups.
 */
public class GroupUserManagementUtil {
  private GroupUserManagementUtil() {}

  public static void dropUserQuietly(Core core, UserManager users, String name) {
    try {
      users.dropUser(name);
    } catch (UserNotFoundException e) {
      // that's fine!
    }
    ConsistencyUtil.waitUntilUserDropped(core, LOCAL.alias(), name);
  }

  public static void dropGroupQuietly(Core core, UserManager users, String name) {
    try {
      users.dropGroup(name);
    } catch (GroupNotFoundException e) {
      // that's fine!
    }
    ConsistencyUtil.waitUntilGroupDropped(core, name);
  }
}
