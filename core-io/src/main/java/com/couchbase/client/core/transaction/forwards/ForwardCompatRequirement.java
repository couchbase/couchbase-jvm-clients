/*
 * Copyright 2022 Couchbase, Inc.
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
package com.couchbase.client.core.transaction.forwards;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;

import java.util.Objects;

@Stability.Internal
abstract class ForwardCompatRequirement {
    public final ForwardCompatBehaviourFull behaviour;

    abstract public ForwardCompatBehaviourFull behaviour(CoreTransactionsSupportedExtensions supported);

    ForwardCompatRequirement(JsonNode json) {
        behaviour = new ForwardCompatBehaviourFull(Objects.requireNonNull(json));
    }
}

/**
 * A particular extension is required.
 */
@Stability.Internal
class ForwardCompatExtensionRequirement extends ForwardCompatRequirement {
    public final String extensionId;

    ForwardCompatExtensionRequirement(JsonNode json) {
        super(Objects.requireNonNull(json));
        extensionId = json.path("e").textValue();
    }

    public ForwardCompatBehaviourFull behaviour(CoreTransactionsSupportedExtensions supported) {
        for (CoreTransactionsExtension e : supported.extensions) {
            if (e.value().equals(extensionId)) {
                return ForwardCompatBehaviourFull.CONTINUE;
            }
        }
        return behaviour;
    }
}

/**
 * A particular protocol version is required.
 */
@Stability.Internal
class ForwardCompatProtocolRequirement extends ForwardCompatRequirement {
    public final int minProtocolMajor;
    public final int minProtocolMinor;

    ForwardCompatProtocolRequirement(JsonNode json) {
        super(Objects.requireNonNull(json));
        String protocolVersion = json.path("p").textValue();
        String[] split = protocolVersion.split("\\.");
        minProtocolMajor = Integer.parseInt(split[0]);
        minProtocolMinor = Integer.parseInt(split[1]);
    }

    public ForwardCompatBehaviourFull behaviour(CoreTransactionsSupportedExtensions supported) {
        if (supported.protocolMajor > minProtocolMajor) {
            return ForwardCompatBehaviourFull.CONTINUE;
        }
        if (supported.protocolMajor < minProtocolMajor) {
            return behaviour;
        }
        if (supported.protocolMinor < minProtocolMinor) {
            return behaviour;
        }
        return ForwardCompatBehaviourFull.CONTINUE;
    }
}
