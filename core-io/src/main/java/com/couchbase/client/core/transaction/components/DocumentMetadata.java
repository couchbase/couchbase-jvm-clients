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
package com.couchbase.client.core.transaction.components;

import com.couchbase.client.core.annotation.Stability;

import java.util.Objects;
import java.util.Optional;

/**
 * Stores some $document metadata from when the document is fetched
 *
 * @author Graham POple
 */
@Stability.Internal
public class DocumentMetadata {
    private final Optional<String> cas;
    private final Optional<String> revid;
    private final Optional<Long> exptime;
    private final Optional<String> crc32;

    public DocumentMetadata(Optional<String> cas,
                            Optional<String> revid,
                            Optional<Long> exptime,
                            Optional<String> crc32) {
        this.cas = Objects.requireNonNull(cas);
        this.revid = Objects.requireNonNull(revid);
        this.exptime = Objects.requireNonNull(exptime);
        this.crc32 = Objects.requireNonNull(crc32);
    }

    public Optional<String> cas() {
        return cas;
    }

    public Optional<String> revid() {
        return revid;
    }

    public Optional<Long> exptime() {
        return exptime;
    }

    public Optional<String> crc32() {
        return crc32;
    }
}
