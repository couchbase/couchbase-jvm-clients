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

package com.couchbase.client.java;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.env.ClusterEnvironment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * Verifies the basic functionality of the {@link ReactiveScope}.
 */
class ReactiveScopeTest {

  @Test
  void preventsDefaultCollectionFromBeingOpened() {
    ReactiveScope scope = new ReactiveScope(
      new AsyncScope("foo", "bar", mock(Core.class), mock(ClusterEnvironment.class))
    );
    assertThrows(IllegalArgumentException.class, () -> scope.collection(CollectionIdentifier.DEFAULT_COLLECTION));
  }

  @Test
  void shouldNotAcceptEmptyOrNullCollectionName() {
    ReactiveScope scope = new ReactiveScope(
      new AsyncScope("foo", "bar", mock(Core.class), mock(ClusterEnvironment.class))
    );
    assertThrows(IllegalArgumentException.class, () -> scope.collection(null));
    assertThrows(IllegalArgumentException.class, () -> scope.collection(""));
  }

}