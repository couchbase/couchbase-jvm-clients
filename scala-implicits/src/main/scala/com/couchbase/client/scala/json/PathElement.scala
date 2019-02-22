package com.couchbase.client.scala.json

private[scala] sealed trait PathElement

private[scala] case class PathObjectOrField(str: String) extends PathElement

private[scala] case class PathArray(str: String, idx: Int) extends PathElement
