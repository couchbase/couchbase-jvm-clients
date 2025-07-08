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
package com.couchbase.client.scala.encodings

import com.couchbase.client.scala.implicits.Codec
import com.github.plokhotnyuk.jsoniter_scala.macros.named

case class Address(address: String)

object Address {
  implicit val rw: upickle.default.ReadWriter[Address] = upickle.default.macroRW
  implicit val decoder: io.circe.Decoder[Address] =
    io.circe.generic.semiauto.deriveDecoder[Address]
  implicit val encoder: io.circe.Encoder[Address] =
    io.circe.generic.semiauto.deriveEncoder[Address]
}

case class User(name: String, age: Int, addresses: Seq[Address])

object User {
  implicit val codec: Codec[User]                   = Codec.codec[User]
  implicit val rw: upickle.default.ReadWriter[User] = upickle.default.macroRW

  import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
  import com.github.plokhotnyuk.jsoniter_scala.macros.{CodecMakerConfig, JsonCodecMaker}
  implicit val codecJsoniter: JsonValueCodec[User] = JsonCodecMaker.make[User](CodecMakerConfig)

  implicit val decoder: io.circe.Decoder[User] = io.circe.generic.semiauto.deriveDecoder[User]
  implicit val encoder: io.circe.Encoder[User] = io.circe.generic.semiauto.deriveEncoder[User]
}

case class User2(
  @named("name") n: String,
  @named("age") a: Int,
  @named("addresses") s: Seq[Address]
)
object User2 {
  implicit val codec: Codec[User2] = Codec.codec
}
