package com.couchbase.client.scala.document

import java.lang.reflect.{ParameterizedType, Type}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.reflect.runtime.universe._
import upickle.default._
import upickle.default.{ReadWriter => RW, macroRW}

object DefaultDecoder {
  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

//  def decode[T](value: String)(implicit tag: TypeTag[T]): T = {
//    read[T](value)
//  }

//  def decode[T](value: String)(implicit tag: TypeTag[T]): T = {
//    //    mapper.readValue(value, typeReference[T](tag))
//    val x = tag.tpe match {
//      case TypeRef(x, s, args) =>
//        println(x)
//        println(s)
//        println(args)
//        s.asClass.info.
//    }
//    mapper.readValue(value, x)
//  }

  //  def decode[T: Manifest](value: Array[Byte]) : T =
//    mapper.readValue(value, typeReference[T])

//  private [this] def typeReference[T: Manifest]: TypeReference[T] = new TypeReference[T] {
//    override def getType: Type = typeFromManifest(manifest[T])
//  }

//  private [this] def typeReference[T](implicit tag: TypeTag[T]): TypeReference[T] = new TypeReference[T] {
//    override def getType: Type = typeFromTypeTag(tag)
//  }

//  private [this] def typeFromManifest(m: Manifest[_]): Type = {
//    if (m.typeArguments.isEmpty) { m.runtimeClass }
//    else new ParameterizedType {
//      override def getRawType: Class[_] = m.runtimeClass
//
//      override def getActualTypeArguments: Array[Type] = m.typeArguments.map(typeFromManifest).toArray
//
//      override def getOwnerType: Null = null
//    }
//  }

//  private [this] def typeFromClassTag(m: ClassTag[_]): Type = {
//    if (m.typeArguments.isEmpty) { m.runtimeClass }
//    else new ParameterizedType {
//      override def getRawType: Class[_] = m.runtimeClass
//
//      override def getActualTypeArguments: Array[Type] = m.typeArguments.map(v => typeFromClassTag(v.).toArray
//
//      override def getOwnerType: Null = null
//    }
//  }

//  private [this] def typeFromTypeTag[T](tag: TypeTag[T]): Type = {
//    tag.tpe match {
//      case TypeRef(x, s, args) =>
//        println(x)
//        println(s)
//        println(args)
//        s.asClass
////        if (args.isEmpty) x.getClass
////        else new ParameterizedType {
////          override def getRawType: Class[_] = x.getClass
////
////          override def getActualTypeArguments: Array[Type] = args.map(v => typeFromTypeTag(tag)).toArray
////
////          override def getOwnerType: Null = null
////        }
//    }
//  }
}
