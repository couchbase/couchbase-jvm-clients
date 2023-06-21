/*
 * Copyright (c) 2023 Couchbase, Inc.
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

package com.couchbase.client.performer.scala.eventing

// [skip:<1.2.4]

import com.couchbase.client.performer.scala.util.OptionsUtil.{DefaultManagementTimeout, convertDuration}
import com.couchbase.client.protocol.run.Result
import com.couchbase.client.protocol.sdk.Command
import com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.{EventingFunction, EventingFunctionKeyspace}
import com.couchbase.client.scala.manager.eventing
import com.couchbase.client.scala.{Cluster, ReactiveCluster}
import reactor.core.scala.publisher.SMono

import scala.util.{Failure, Success}

object EventingHelper {

    def handleEventingFunctionManager(cluster: Cluster, command: Command): Result.Builder = {
        val efm = command.getClusterCommand.getEventingFunctionManager

        val result = Result.newBuilder()

        if (efm.hasGetFunction) {
            val name = efm.getGetFunction.getName


            val response = cluster.eventingFunctions.getFunction(name,
                if (efm.getGetFunction.hasOptions && efm.getGetFunction.getOptions.hasTimeout) convertDuration(efm.getGetFunction.getOptions.getTimeout)
                else DefaultManagementTimeout)
            response match {
                case Success(eventingFunction: com.couchbase.client.scala.manager.eventing.EventingFunction) => minimalEventingFunctionFromResult(result, eventingFunction)
                case Failure(e) => throw e
            }

        } else {
            throw new UnsupportedOperationException(new IllegalArgumentException("Unknown operation"))
        }
    }

    def handleEventingFunctionManagerReactive(cluster: ReactiveCluster, command: Command): SMono[Result] = {
        val efm = command.getClusterCommand.getEventingFunctionManager

        val result = Result.newBuilder()

        if (efm.hasGetFunction) {
            val name = efm.getGetFunction.getName
            val response: SMono[eventing.EventingFunction] = cluster.eventingFunctions.getFunction(name,
                if (efm.getGetFunction.hasOptions && efm.getGetFunction.getOptions.hasTimeout) convertDuration(efm.getGetFunction.getOptions.getTimeout)
                else DefaultManagementTimeout)

            response.map(r => {
                minimalEventingFunctionFromResult(result, r)
                result.build()
            })
        } else {
            SMono.error(new IllegalArgumentException("Unknown operation"))
        }
    }

    private def minimalEventingFunctionFromResult(result: Result.Builder, response: com.couchbase.client.scala.manager.eventing.EventingFunction): Result.Builder = {
        val builder = EventingFunction.newBuilder()

        builder.setName(response.name)
                .setCode(response.code)
                .setMetadataKeyspace(convertKeyspace(response.metadataKeyspace))
                .setSourceKeyspace(convertKeyspace(response.sourceKeyspace))


        result.setSdk(com.couchbase.client.protocol.sdk.Result.newBuilder()
                .setEventingFunctionManagerResult(com.couchbase.client.protocol.sdk.cluster.eventingfunctionmanager.Result.newBuilder()
                        .setEventingFunction(builder)))
    }

    private def convertKeyspace(oldKeyspace: com.couchbase.client.scala.manager.eventing.EventingFunctionKeyspace): EventingFunctionKeyspace = {
        val newKeyspace = EventingFunctionKeyspace.newBuilder()

        newKeyspace.setBucket(oldKeyspace.bucket).setScope(oldKeyspace.scope.orNull).setCollection(oldKeyspace.collection.orNull)
        newKeyspace.build()
    }
}
