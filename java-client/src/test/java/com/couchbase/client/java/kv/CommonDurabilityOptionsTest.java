/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java.kv;

import com.couchbase.client.core.msg.kv.DurabilityLevel;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommonDurabilityOptionsTest {

  /**
   * Regression test to make sure that it is not possible to set both durability mechanisms at the same time.
   */
  @Test
  void overridesConflictingDurability() {
    TestDurabilityOptions options = new TestDurabilityOptions();
    options.durability(DurabilityLevel.MAJORITY);
    options.durability(PersistTo.ACTIVE, ReplicateTo.ONE);
    assertEquals(Optional.empty(), options.build().durabilityLevel());

    options = new TestDurabilityOptions();
    options.durability(PersistTo.ACTIVE, ReplicateTo.ONE);
    options.durability(DurabilityLevel.MAJORITY);
    assertEquals(PersistTo.NONE, options.build().persistTo());
    assertEquals(ReplicateTo.NONE, options.build().replicateTo());
  }

  static class TestDurabilityOptions extends CommonDurabilityOptions<TestDurabilityOptions> {

    BuiltCommonDurabilityOptions build() {
      return new BuiltTestDurabilityOptions();
    }

    class BuiltTestDurabilityOptions extends BuiltCommonDurabilityOptions { }

  }
}