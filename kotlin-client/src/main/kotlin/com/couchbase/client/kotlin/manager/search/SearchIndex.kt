/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.kotlin.manager.search

import com.couchbase.client.core.api.manager.search.CoreSearchIndex
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi


/**
 * A Full-Text Search index definition.
 */
@VolatileCouchbaseApi
public class SearchIndex(
    public val name: String,
    public val sourceName: String,
    public val uuid: String? = null,
    public val type: String? = null,
    public val params: Map<String, Any?> = emptyMap(),
    public val sourceUuid: String? = null,
    public val sourceParams: Map<String, Any?> = emptyMap(),
    public val sourceType: String? = null,
    public val planParams: Map<String, Any?> = emptyMap(),
) {
    internal constructor(core: CoreSearchIndex) : this(
        name = core.name(),
        sourceName = core.sourceName(),
        uuid = core.uuid(),
        type = core.type(),
        params = core.params() ?: emptyMap(),
        sourceUuid = core.sourceUuid(),
        sourceParams = core.sourceParams() ?: emptyMap(),
        sourceType = core.sourceType(),
        planParams = core.planParams() ?: emptyMap(),
    )

    public fun copy(
        name: String = this.name,
        sourceName: String = this.sourceName,
        uuid: String? = this.uuid,
        type: String? = this.type,
        params: Map<String, Any?> = this.params,
        sourceUuid: String? = this.sourceUuid,
        sourceParams: Map<String, Any?> = this.sourceParams,
        sourceType: String? = this.sourceType,
        planParams: Map<String, Any?> = this.planParams,
    ): SearchIndex = SearchIndex(
        name = name,
        sourceName = sourceName,
        uuid = uuid,
        type = type,
        params = params,
        sourceUuid = sourceUuid,
        sourceParams = sourceParams,
        sourceType = sourceType,
        planParams = planParams
    )

    public fun toJson(): String = this.toCore().toJson()

    override fun toString(): String {
        return "SearchIndex(name='$name', sourceName='$sourceName', uuid=$uuid, type=$type, params=$params, sourceUuid=$sourceUuid, sourceParams=$sourceParams, sourceType=$sourceType, planParams=$planParams)"
    }

    public companion object {
        /**
         * Takes an JSON-encoded index definition and turns it into a [SearchIndex].
         *
         * @param json the encoded JSON index definition.
         * @return the instantiated index.
         */
        public fun fromJson(json: String): SearchIndex {
            return SearchIndex(CoreSearchIndex.fromJson(json))
        }
    }
}
