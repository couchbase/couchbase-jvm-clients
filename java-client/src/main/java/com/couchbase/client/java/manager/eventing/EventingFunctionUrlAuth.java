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

package com.couchbase.client.java.manager.eventing;

/**
 * Abstract interface for all possible URL Binding authentication mechanisms.
 * <p>
 * Supported are:
 * <ul>
 *   <li>No Authentication: {@link #noAuth()}</li>
 *   <li>Basic: {@link #basicAuth(String, String)}</li>
 *   <li>Digest: {@link #digestAuth(String, String)}</li>
 *   <li>Bearer: {@link #bearerAuth(String)}</li>
 * </ul>
 */
public abstract class EventingFunctionUrlAuth {

    /**
     * No authentication will be used.
     *
     * @return the no auth instance indicating no auth.
     */
    public static EventingFunctionUrlNoAuth noAuth() {
        return new EventingFunctionUrlNoAuth();
    }

    /**
     * HTTP Basic Authentication will be used.
     *
     * @param username the username for authentication.
     * @param password the password for authentication.
     * @return the basic instance indicating http basic auth.
     */
    public static EventingFunctionUrlAuthBasic basicAuth(String username, String password) {
        return new EventingFunctionUrlAuthBasic(username, password);
    }

    /**
     * HTTP Digest Authentication will be used.
     *
     * @param username the username for authentication.
     * @param password the password for authentication.
     * @return the digest instance indicating http digest auth.
     */
    public static EventingFunctionUrlAuthDigest digestAuth(String username, String password) {
        return new EventingFunctionUrlAuthDigest(username, password);
    }

    /**
     * HTTP Bearer / Token Authentication will be used.
     *
     * @param key the bearer key/token for authentication.
     * @return the bearer instance indicating http bearer auth.
     */
    public static EventingFunctionUrlAuthBearer bearerAuth(String key) {
        return new EventingFunctionUrlAuthBearer(key);
    }

}
