package com.couchbase.client.scala.util

import ujson.ParseException

/** Customizes upickle serialization for our needs.
  */
object CouchbasePickler extends upickle.AttributeTagged {
  // upickle writes Options as [] and ["value"] by default, which isn't that useful
  // IntelliJ complains about this, but the compiler is fine
  override implicit def OptionWriter[T: Writer]: Writer[Option[T]] =
    implicitly[Writer[T]].comap[Option[T]] {
      case None => null.asInstanceOf[T]
      case Some(x) => x
    }

  override implicit def OptionReader[T: Reader]: Reader[Option[T]] =
    implicitly[Reader[T]].mapNulls{
      case null => None
      case x => Some(x)
    }
}

