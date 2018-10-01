/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Provides more assertions over the basic junit 5 ones.
 */
public class Assertions {

  /**
   * Asserts if a thread with the given name is running.
   *
   * @param name the name of the thread
   */
  public static void assertThreadRunning(final String name) {
    assertTrue(Utils.threadRunning(name), "Thread with name \"" + name + "\" not running");
  }

  /**
   * Asserts if a thread with the given name is not running.
   *
   * @param name the name of the thread
   */
  public static void assertThreadNotRunning(final String name) {
    assertFalse(Utils.threadRunning(name), "Thread with name \"" + name + "\" running");
  }
}
