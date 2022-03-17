/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.core.transaction.cleanup;

import com.couchbase.client.core.annotation.Stability;

import java.util.List;
import java.util.Objects;

/**
 * Utility class to store the result of a client checking/updating the Client Record doc.
 *
 * @author Graham Pople
 */
@Stability.Internal
public class ClientRecordDetails {
    private final int numActiveClients;
    private final int indexOfThisClient;
    private final boolean clientIsNew;
    private final List<String> expiredClientIds;
    private final int numExistingClients;
    private final int numExpiredClients;
    private final boolean overrideEnabled;
    private final long overrideExpires;
    private final long casNowNanos;

    public ClientRecordDetails(int numActiveClients,
                               int indexOfThisClient,
                               boolean clientIsNew,
                               List<String> expiredClientIds,
                               int numExistingClients,
                               int numExpiredClients,
                               boolean overrideEnabled,
                               long overrideExpires,
                               long casNowNanos) {
        this.numActiveClients = numActiveClients;
        this.indexOfThisClient = indexOfThisClient;
        this.clientIsNew = clientIsNew;
        this.expiredClientIds = Objects.requireNonNull(expiredClientIds); // defensive copy is overkill for an internal class
        this.numExistingClients = numExistingClients;
        this.numExpiredClients = numExpiredClients;
        this.overrideEnabled = overrideEnabled;
        this.overrideExpires = overrideExpires;
        this.casNowNanos = casNowNanos;
    }

    public int numActiveClients() {
        return numActiveClients;
    }

    public int indexOfThisClient() {
        return indexOfThisClient;
    }

    public boolean clientIsNew() {
        return clientIsNew;
    }

    public List<String> expiredClientIds() {
        return expiredClientIds;
    }

    public int numExistingClients() {
        return numExistingClients;
    }

    public int numExpiredClients() {
        return numExpiredClients;
    }

    public boolean overrideEnabled() {
        return overrideEnabled;
    }

    public long overrideExpires() {
        return overrideExpires;
    }

    public long casNow() {
        return casNowNanos;
    }

    public boolean overrideActive() {
        return overrideEnabled && casNowNanos < overrideExpires;
    }
}
