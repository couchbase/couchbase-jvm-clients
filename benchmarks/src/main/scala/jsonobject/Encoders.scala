package jsonobject

import com.couchbase.client.core.error.DecodingFailedException
import com.couchbase.client.java.json.JacksonTransformers
import com.couchbase.client.scala.codec.Conversions.{BinaryEncodeParams, Decodable, Encodable, JsonEncodeParams}
import com.couchbase.client.scala.document.DecodeParams
import com.couchbase.client.scala.json.JsonObject
import experiments.JsoniterObject
import io.netty.util.CharsetUtil

import scala.util.{Failure, Success, Try}

// Multiple methods for encoding JsonObject
object Encoders {

//    implicit object JsonObjectJawn extends Encodable[JsonObject] {
//      override def encode(content: JsonObject) = {
//        Try(org.json4s.jackson.JsonMethods.compact(content).getBytes(CharsetUtil.UTF_8)).map((_, JsonEncodeParams))
//      }
//  }

  implicit object JsonObjectExperimentConvert extends Encodable[JsonObjectExperiment] {
    override def encode(content: JsonObjectExperiment) = {
      Try(JacksonTransformers.MAPPER.writeValueAsBytes(content), JsonEncodeParams)
    }
  }


  implicit object JsoniterObjectConvert extends Encodable[JsoniterObject] {
    override def encode(content: JsoniterObject) = {
      Try(JsoniterTransformers.encode(content), JsonEncodeParams)
    }
  }
}

object Decoders {
  implicit object JsoniterObjectConvert extends Decodable[JsoniterObject] {
    override def decode(bytes: Array[Byte], params: DecodeParams) = {
      val out = Try(JsoniterTransformers.decode(bytes))
      out match {
        case Success(_) => out
        case Failure(err) => Failure(new DecodingFailedException(err))
      }
    }
  }

  implicit object JsonObjectExperiment extends Decodable[JsonObjectExperiment] {
    override def decode(bytes: Array[Byte], params: DecodeParams) = {
      val out = Try(JacksonTransformers.MAPPER.readValue(bytes, classOf[JsonObjectExperiment]))
      out match {
        case Success(_) => out
        case Failure(err) => Failure(new DecodingFailedException(err))
      }
    }
  }

}