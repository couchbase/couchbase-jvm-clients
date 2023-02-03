/*
 * Copyright 2023 Couchbase, Inc.
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

package com.couchbase.client.core.api.shared;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.util.CbCollections;

import java.util.Iterator;
import java.util.List;

@Stability.Internal
public class CoreMutationState implements Iterable<MutationToken> {
  private final List<MutationToken> tokens;

  public CoreMutationState(Iterable<MutationToken> tokens) {
    this.tokens = CbCollections.listCopyOf(tokens);
  }

  public List<MutationToken> tokens() {
    return tokens;
  }

  @Override
  public Iterator<MutationToken> iterator() {
    return tokens.iterator();
  }
}
