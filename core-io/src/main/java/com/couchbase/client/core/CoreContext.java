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
package com.couchbase.client.core;

import com.couchbase.client.core.cnc.AbstractContext;
import com.couchbase.client.core.env.CoreEnvironment;

import java.util.Map;

/**
 * The {@link CoreContext} is bound to a core and provides both exportable and
 * usable state for the rest of the application to use.
 */
public class CoreContext extends AbstractContext {

    /**
     * A (app local) unique ID per core instance.
     */
    private final long id;

    /**
     * The attached environment for this core.
     */
    private final CoreEnvironment env;

    /**
     * Creates a new {@link CoreContext}.
     *
     * @param id the core id.
     * @param env the core environment.
     */
    public CoreContext(final long id, final CoreEnvironment env) {
        this.id = id;
        this.env = env;
    }

    /**
     * A (app local) unique ID per core instance.
     *
     * @return the app local id.
     */
    public long id() {
        return id;
    }

    /**
     * The attached environment for this core.
     *
     * @return the core environment attached.
     */
    public CoreEnvironment env() {
        return env;
    }

    @Override
    protected void injectExportableParams(final Map<String, Object> input) {
        input.put("core", id);
    }
}
