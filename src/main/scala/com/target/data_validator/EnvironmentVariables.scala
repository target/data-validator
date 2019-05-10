package com.target.data_validator

import scala.collection.mutable
import scala.util.Try

object EnvironmentVariables {
  type MaybeEnvVar = Try[Option[String]]

  val accessedEnvVars: mutable.Map[String, MaybeEnvVar] = mutable.Map.empty
  private[this] def clearAccessList(): Unit = accessedEnvVars.clear()

  def get(key: String): MaybeEnvVar = {
    val result = Try(System.getenv(key)).map(Option(_))
    accessedEnvVars += key -> result
    result
  }

  def getWithHandlers[T](key: String)(whenError: PartialFunction[Throwable, T],
                                      whenUnset: => T,
                                      whenPresent: String => T
  ): Try[T] = {
    get(key)
      .map(_.map(whenPresent).getOrElse(whenUnset))
      .recover(whenError)
  }
}
