/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.client.scala.util

import com.couchbase.client.core.api.manager.search.CoreSearchIndex
import com.couchbase.client.core.api.query.CoreReactiveQueryResult
import com.couchbase.client.scala.manager.search.SearchIndex
import com.couchbase.client.scala.query.ReactiveQueryResult
import reactor.core.publisher.{Flux, Mono}
import reactor.core.scala.publisher.{SFlux, SMono}

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.language.reflectiveCalls

private[scala] object CoreCommonConvertersScala2 {
  def convert(in: CoreReactiveQueryResult): ReactiveQueryResult = {
    ReactiveQueryResult(
      FutureConversions.javaFluxToScalaFlux(in.rows),
      FutureConversions
        .javaMonoToScalaMono(in.metaData)
        .map(md => CoreCommonConverters.convert(md))
    )
  }

  def convert[T](in: Mono[T]): SMono[T] = {
    FutureConversions.javaMonoToScalaMono(in)
  }

  def convert[T](in: Flux[T]): SFlux[T] = {
    FutureConversions.javaFluxToScalaFlux(in)
  }

  def convert(in: SearchIndex): CoreSearchIndex = {
    CoreSearchIndex.fromJson(in.toJson)
  }

  def convert(in: CoreSearchIndex): SearchIndex = {
    SearchIndex(
      in.name,
      in.sourceName,
      Option(in.uuid),
      Option(in.`type`),
      Option(CoreCommonConverters.convert(in.params.asScala.toMap)),
      Option(in.sourceUuid),
      Option(
        CoreCommonConverters.convert(Option(in.sourceParams).map(_.asScala).getOrElse(Map()).toMap)
      ),
      Option(in.sourceType),
      Option(CoreCommonConverters.convert(in.planParams.asScala.toMap))
    )
  }
}
