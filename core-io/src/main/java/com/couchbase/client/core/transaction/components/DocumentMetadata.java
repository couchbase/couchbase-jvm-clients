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
    private final String cas;
    private final String revid;
    private final Long exptime;

    public DocumentMetadata(String cas,
                            String revid,
                            Long exptime) {
        this.cas = cas;
        this.revid = revid;
        this.exptime = exptime;
    }

    public String cas() {
        return cas;
    }

    public String revid() {
        return revid;
    }

    public Long exptime() {
        return exptime;
    }
    @Override
    public String toString() {
        return "DocumentMetadata{" +
                "cas=" + cas +
                ", revid=" + revid +
                ", exptime=" + exptime +
                '}';
    }
}
