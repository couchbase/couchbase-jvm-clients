package com.couchbase.client.performer.scala.util

import java.io.{ByteArrayOutputStream, NotSerializableException, ObjectOutputStream}

object SerializableValidation {
  /** Anything used by the Spark Connector needs to be Serializable */
  def assertIsSerializable(input: Any): Unit = {
    try {
      val bOutput = new ByteArrayOutputStream(2048)
      val objectOutputStream = new ObjectOutputStream(bOutput)
      objectOutputStream.writeObject(input)
      objectOutputStream.close()
    }
    catch {
      case e: NotSerializableException =>
        throw new RuntimeException(s"Failed to serialize object.  This will cause issues for the Spark Connector such as JVMCBC-1458.  Object was ${input}", e)
    }
  }
}
