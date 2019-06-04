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

package com.couchbase.client.core.service.kv;

import com.couchbase.client.core.error.AuthenticationException;
import com.couchbase.client.core.error.CASMismatchException;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.msg.kv.ObserveViaCasResponse;
import com.couchbase.client.core.msg.kv.ObserveViaSeqnoResponse;
import com.couchbase.client.core.msg.kv.ResponseStatusDetails;

class ObserveItem {

  private final int replicated;
  private final int persisted;
  private final boolean persistedActive;

  private ObserveItem(final int replicated, final int persisted, final boolean persistedActive) {
    this.replicated = replicated;
    this.persisted = persisted;
    this.persistedActive = persistedActive;
  }


  public static ObserveItem empty() {
    return new ObserveItem(0, 0, false);
  }

  public static ObserveItem fromCas(String id, long cas, boolean remove,
                                    ObserveViaCasResponse response,
                                    ObserveViaCasResponse.ObserveStatus persistIdentifier,
                                    ObserveViaCasResponse.ObserveStatus replicaIdentifier) {

    int replicated = 0;
    int persisted = 0;
    boolean persistedMaster = false;

    ObserveViaCasResponse.ObserveStatus status = response.observeStatus();

    if (response.status() == ResponseStatus.NO_ACCESS) {
      String details = ResponseStatusDetails.stringify(response.status(), response.statusDetails());
      throw new AuthenticationException("The application is not authorized to perform the \"observe\" "
        + "operation, make sure you have read privileges on this bucket: " + details);
    }

    // the CAS values always need to match up to make sure we are still observing the right
    // document. The only exclusion from that rule is when a real delete is returned, because
    // then the cas value is 0.
    boolean validCas = cas == response.cas()
      || (remove && response.cas() == 0 && status == persistIdentifier);

    if (response.active()) {
      if (!validCas) {
        throw CASMismatchException.forKey(id);
      }

      if (status == persistIdentifier) {
        persisted++;
        persistedMaster = true;
      }
    } else if (validCas) {
      if (status == persistIdentifier) {
        persisted++;
        replicated++;
      } else if (status == replicaIdentifier) {
        replicated++;
      }
    }

    return new ObserveItem(replicated, persisted, persistedMaster);
  }

  public static ObserveItem fromMutationToken(final MutationToken token, final ObserveViaSeqnoResponse response) {
    boolean replicated = response.currentSeqNo() >= token.sequenceNumber();
    boolean persisted = response.lastPersistedSeqNo() >= token.sequenceNumber();

    return new ObserveItem(
      replicated && !response.active() ? 1 : 0,
      persisted ? 1 : 0,
      response.active() && persisted
    );
  }

  public ObserveItem add(final ObserveItem other) {
    return new ObserveItem(
      this.replicated + other.replicated,
      this.persisted + other.persisted,
      this.persistedActive || other.persistedActive
    );
  }

  public boolean check(final Observe.ObservePersistTo persistTo, final Observe.ObserveReplicateTo replicateTo) {
    boolean persistDone = false;
    boolean replicateDone = false;
    if (persistTo == Observe.ObservePersistTo.ACTIVE) {
      if (persistedActive) {
        persistDone = true;
      }
    } else if (persisted >= persistTo.value()) {
      persistDone = true;
    }
    if (replicated >= replicateTo.value()) {
      replicateDone = true;
    }
    return persistDone && replicateDone;
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("persisted ").append(persisted);
    if (persistedActive)
      sb.append(" (active)");
    sb.append(", replicated ").append(replicated);
    return sb.toString();
  }
}
