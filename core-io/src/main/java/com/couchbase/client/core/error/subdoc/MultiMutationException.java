/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.client.core.error.subdoc;

import java.util.List;

/**
 * Exception denoting that at least one error occurred when applying
 * multiple mutations using the sub-document API.
 * TODO currently we always do subdoc as multi, even if just 1 op, so may need to rewrite this
 * None of the mutations were applied.
 *
 * @author Simon Baslé
 * @since 2.2
 */
public class MultiMutationException extends SubDocumentException {

    private final List<SubDocumentException> childExceptions;

    public MultiMutationException(List<SubDocumentException> childExceptions) {
        super("Multiple mutation could not be applied.");
        this.childExceptions = childExceptions;
    }

    /**
     * @return the list of {@link SubDocumentException} individual errors .
     */
    public List<SubDocumentException> childExceptions() {
        return childExceptions;
    }
}
