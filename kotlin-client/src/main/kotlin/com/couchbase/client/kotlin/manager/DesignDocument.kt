/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.manager

import com.couchbase.client.core.manager.CoreViewIndexManager
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi

@VolatileCouchbaseApi
public class DesignDocument(
    name: String,
    views: Map<String, View> = emptyMap()
) {
    public val name : String = CoreViewIndexManager.requireUnqualifiedName(name)
    public val views: Map<String, View> = views.toMap()

    public fun withView(name: String, view: View): DesignDocument =
        copy(views = views + (name to view))

    public fun withoutView(name: String): DesignDocument =
        copy(views = views - (name))

    public operator fun minus(viewName: String): DesignDocument =
        withoutView(viewName)

    public fun copy(
        name: String = this.name,
        views: Map<String, View> = this.views
    ) : DesignDocument = DesignDocument(name, views)

    override fun toString(): String {
        return "DesignDocument(name='$name', views=$views)"
    }
}
