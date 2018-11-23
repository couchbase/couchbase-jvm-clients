// TODO requires some bits from core

///*
// * Copyright (c) 2016 Couchbase, Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package com.couchbase.client.scala.transcoder
//
//import java.io.IOException
//import java.math.BigDecimal
//
//import com.couchbase.client.scala.document.{JsonArray, JsonObject}
//import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, JsonToken, Version}
//import com.fasterxml.jackson.databind.{JsonDeserializer, JsonSerializer, ObjectMapper, SerializerProvider}
//import com.fasterxml.jackson.databind.module.SimpleModule
//
//
//object JacksonTransformers {
//  val MAPPER = new ObjectMapper
//  val JSON_VALUE_MODULE = new SimpleModule("JsonValueModule", new Version(1, 0, 0, null, null, null))
//
//  private class JsonObjectSerializer extends JsonSerializer[JsonObject] {
//    @throws[IOException]
//    override def serialize(value: JsonObject, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
//      jgen.writeObject(value.content)
//    }
//  }
//
//  // TODO
////  private class JsonArraySerializer extends JsonSerializer[JsonArray] {
////    @throws[IOException]
////    override def serialize(value: JsonArray, jgen: JsonGenerator, provider: SerializerProvider): Unit = {
////      jgen.writeObject(value.toList)
////    }
////  }
//
//  abstract private class AbstractJsonValueDeserializer[T]() extends JsonDeserializer[T] {
//    val decimalForFloat = System.getProperty("com.couchbase.json.decimalForFloat", "false").toBoolean
//
//    @throws[IOException]
//    protected def decodeObject(parser: JsonParser, target: JsonObject): JsonObject = {
//      var current = parser.nextToken
//      var field: String = null
//      while (current != null && (current ne JsonToken.END_OBJECT)) {
//        if (current eq JsonToken.START_OBJECT) target.put(field, decodeObject(parser, JsonObject.empty))
////        else if (current eq JsonToken.START_ARRAY) target.put(field, decodeArray(parser, JsonArray.empty))
//        else if (current eq JsonToken.FIELD_NAME) field = parser.getCurrentName
//        else current match {
//          case VALUE_TRUE =>
//          case VALUE_FALSE =>
//            target.put(field, parser.getBooleanValue)
//            break //todo: break is not supported
//          case VALUE_STRING =>
//            target.put(field, parser.getValueAsString)
//            break //todo: break is not supported
//          case VALUE_NUMBER_INT =>
//          case VALUE_NUMBER_FLOAT =>
//            var numberValue = parser.getNumberValue
//            if (numberValue.isInstanceOf[Double] && decimalForFloat) numberValue = parser.getDecimalValue
//            target.put(field, numberValue)
//            break //todo: break is not supported
//          case VALUE_NULL =>
//            target.put(field, null.asInstanceOf[JsonObject])
//            break //todo: break is not supported
//          case _ =>
//            throw new IllegalStateException("Could not decode JSON token: " + current)
//        }
//        current = parser.nextToken
//      }
//      target
//    }
//
////    @throws[IOException]
////    protected def decodeArray(parser: JsonParser, target: JsonArray): JsonArray = {
////      var current = parser.nextToken
////      while ( {
////        current != null && (current ne JsonToken.END_ARRAY)
////      }) {
////        if (current eq JsonToken.START_OBJECT) target.add(decodeObject(parser, JsonObject.empty))
////        else if (current eq JsonToken.START_ARRAY) target.add(decodeArray(parser, JsonArray.empty))
////        else current match {
////          case VALUE_TRUE =>
////          case VALUE_FALSE =>
////            target.add(parser.getBooleanValue)
////            break //todo: break is not supported
////          case VALUE_STRING =>
////            target.add(parser.getValueAsString)
////            break //todo: break is not supported
////          case VALUE_NUMBER_INT =>
////          case VALUE_NUMBER_FLOAT =>
////            var numberValue = parser.getNumberValue
////            if (numberValue.isInstanceOf[Double] && decimalForFloat) numberValue = parser.getDecimalValue
////            target.add(numberValue)
////            break //todo: break is not supported
////          case VALUE_NULL =>
////            target.add(null.asInstanceOf[JsonObject])
////            break //todo: break is not supported
////          case _ =>
////            throw new IllegalStateException("Could not decode JSON token.")
////        }
////        current = parser.nextToken
////      }
////      target
////    }
//  }
//
//  private class JsonArrayDeserializer extends JacksonTransformers.AbstractJsonValueDeserializer[JsonArray] {
//    @throws[IOException]
//    override def deserialize(jp: JsonParser, ctx: DeserializationContext): JsonArray = if (jp.getCurrentToken eq JsonToken.START_ARRAY) decodeArray(jp, JsonArray.empty)
//    else throw new IllegalStateException("Expecting Array as root level object, " + "was: " + jp.getCurrentToken)
//  }
//
//  private class JsonObjectDeserializer extends JacksonTransformers.AbstractJsonValueDeserializer[JsonObject] {
//    @throws[IOException]
//    override def deserialize(jp: JsonParser, ctx: DeserializationContext): JsonObject = if (jp.getCurrentToken eq JsonToken.START_OBJECT) decodeObject(jp, JsonObject.empty)
//    else throw new IllegalStateException("Expecting Object as root level object, " + "was: " + jp.getCurrentToken)
//  }
//
//  try JSON_VALUE_MODULE.addSerializer(classOf[JsonObject], new JacksonTransformers.JsonObjectSerializer)
//  JSON_VALUE_MODULE.addSerializer(classOf[JsonArray], new JacksonTransformers.JsonArraySerializer)
//  JSON_VALUE_MODULE.addDeserializer(classOf[JsonObject], new JacksonTransformers.JsonObjectDeserializer)
//  JSON_VALUE_MODULE.addDeserializer(classOf[JsonArray], new JacksonTransformers.JsonArrayDeserializer)
//  MAPPER.registerModule(JacksonTransformers.JSON_VALUE_MODULE)
//
//}
//
//class JacksonTransformers private() {
//}
