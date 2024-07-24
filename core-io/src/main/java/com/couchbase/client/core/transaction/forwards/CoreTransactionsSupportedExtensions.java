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
package com.couchbase.client.core.transaction.forwards;

import com.couchbase.client.core.annotation.Stability;

import java.util.Arrays;
import java.util.Set;

import static com.couchbase.client.core.util.CbCollections.newEnumSet;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

/**
 * Defines what is support by this implementation (extensions and protocol version).
 */
@Stability.Internal
public class CoreTransactionsSupportedExtensions {
    public static final CoreTransactionsSupportedExtensions NONE = from();
    public static final CoreTransactionsSupportedExtensions ALL = from(CoreTransactionsExtension.values());

    public final Set<CoreTransactionsExtension> extensions;
    private final Set<String> extensionIds;

    public final int protocolMajor = 2;
    public final int protocolMinor = 1;

    private CoreTransactionsSupportedExtensions(Iterable<CoreTransactionsExtension> extensions) {
        this.extensions = unmodifiableSet(newEnumSet(CoreTransactionsExtension.class, extensions));
        this.extensionIds = unmodifiableSet(
                this.extensions.stream()
                        .map(CoreTransactionsExtension::value)
                        .collect(toSet())
        );
    }

    public static CoreTransactionsSupportedExtensions from(CoreTransactionsExtension... extensions) {
        return new CoreTransactionsSupportedExtensions(Arrays.asList(extensions));
    }

    public boolean has(String extensionId) {
        return extensionIds.contains(extensionId);
    }

    @Override
    public String toString() {
        return "Supported {extensions=" + extensions + ", protocol=" + protocolMajor + "." + protocolMinor + "}";
    }
}
