/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.kotlin.manager.http

import com.couchbase.client.core.msg.RequestTarget
import com.couchbase.client.core.node.NodeIdentifier
import com.couchbase.client.core.service.ServiceType

/**
 * Determines which host and port an HTTP request is dispatched to.
 * Use the factory methods in the companion object to create new instances.
 */
public class HttpTarget internal constructor(
    serviceType: ServiceType,
    nodeIdentifier: NodeIdentifier?,
    bucketName: String? = null,
) {
    internal val coreTarget = RequestTarget(serviceType, nodeIdentifier, bucketName)

    override fun toString(): String = coreTarget.toString()

    /**
     * Returns a copy of this target with a different node.
     *
     * @param nodeIdentifier node to receive requests, or null to let the service locator decide.
     */
    public fun copy(nodeIdentifier: NodeIdentifier? = null): HttpTarget =
        HttpTarget(coreTarget.serviceType(), nodeIdentifier ?: coreTarget.nodeIdentifier(), coreTarget.bucketName())

    public companion object {
        /**
         * Target the Analytics service (port 8095 by default).
         *
         * @param nodeIdentifier node to receive requests, or null to let the service locator decide.
         */
        public fun analytics(nodeIdentifier: NodeIdentifier? = null): HttpTarget =
            HttpTarget(ServiceType.ANALYTICS, nodeIdentifier)

        /**
         * Target the Eventing service (port 8096 by default).
         *
         * @param nodeIdentifier node to receive requests, or null to let the service locator decide.
         */
        public fun eventing(nodeIdentifier: NodeIdentifier? = null): HttpTarget =
            HttpTarget(ServiceType.EVENTING, nodeIdentifier)

        /**
         * Target the Cluster Management service (port 8091 by default).
         *
         * @param nodeIdentifier node to receive requests, or null to let the service locator decide.
         */
        public fun manager(nodeIdentifier: NodeIdentifier? = null): HttpTarget =
            HttpTarget(ServiceType.MANAGER, nodeIdentifier)

        /**
         * Target the N1QL Query service (port 8093 by default).
         *
         * @param nodeIdentifier node to receive requests, or null to let the service locator decide.
         */
        public fun query(nodeIdentifier: NodeIdentifier? = null): HttpTarget =
            HttpTarget(ServiceType.QUERY, nodeIdentifier)

        /**
         * Target the Full-Text Search service (port 8094 by default).
         *
         * @param nodeIdentifier node to receive requests, or null to let the service locator decide.
         */
        public fun search(nodeIdentifier: NodeIdentifier? = null): HttpTarget =
            HttpTarget(ServiceType.SEARCH, nodeIdentifier)

        /**
         * Target the Views service (port 8092 by default).
         *
         * @param bucketName the bucket whose views you want to access.
         * @param nodeIdentifier node to receive requests, or null to let the service locator decide.
         * If non-null, must be hosting active KV partitions for the target bucket.
         */
        public fun views(bucketName: String, nodeIdentifier: NodeIdentifier? = null): HttpTarget =
            HttpTarget(ServiceType.VIEWS, nodeIdentifier, bucketName)
    }
}
