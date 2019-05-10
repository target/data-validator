package com.target.data_validator

import scala.util.Try

object EnvironmentVariables {

  def get(key: String): Try[Option[String]] = {
    Try(System.getenv(key)).map(Option(_))
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
