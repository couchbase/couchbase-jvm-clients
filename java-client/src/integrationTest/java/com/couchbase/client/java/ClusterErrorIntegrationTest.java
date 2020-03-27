/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.core.error.InvalidArgumentException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contains "negative" tests to verify handling of certain error cases at the cluster level.
 */
class ClusterErrorIntegrationTest {

  @Test
  void failsOnInvalidConnectionString() {
    InvalidArgumentException exception = assertThrows(
      InvalidArgumentException.class,
      () -> Cluster.connect("localhost:8091", "foo", "bar")
    );
    assertTrue(exception.getMessage().contains("Please omit the port and use \"localhost\" instead."));

    exception = assertThrows(
      InvalidArgumentException.class,
      () -> Cluster.connect("1.2.3.4:8091", "foo", "bar")
    );
    assertTrue(exception.getMessage().contains("Please omit the port and use \"1.2.3.4\" instead."));
  }

}
