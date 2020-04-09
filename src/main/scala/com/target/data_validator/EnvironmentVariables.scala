package com.target.data_validator

import scala.collection.mutable
import scala.util.Try

object EnvironmentVariables {
  type MaybeEnvVar = Try[Option[String]]

  val accessedEnvVars: mutable.Map[String, MaybeEnvVar] = mutable.Map.empty

  def get(key: String): EnvVarResult = {
    getWithHandlers(key)(
      whenError = { case throwable: Throwable => Inaccessible(throwable) },
      whenUnset = { Unset },
      whenPresent = { Present }
    )
      .recover { case throwable: Throwable => Error(throwable) }
      .get
  }

  def getWithHandlers[T](key: String)(
    whenError: PartialFunction[Throwable, T],
    whenUnset: => T,
    whenPresent: String => T
  ): Try[T] = {
    tryGet(key)
      .map(_.map(whenPresent).getOrElse(whenUnset))
      .recover(whenError)
  }

  def tryGet(key: String): MaybeEnvVar = {
    val result = Try(System.getenv(key)).map(Option(_))
    accessedEnvVars += key -> result
    result
  }

  sealed trait EnvVarResult {
    def toString: String
  }
  case class Present(value: String) extends EnvVarResult {
    override def toString: String = value
  }
  case class Inaccessible(message: String) extends EnvVarResult {
    override val toString: String = s"<inaccessible: $message>"
  }
  object Inaccessible {
    def apply(throwable: Throwable): Inaccessible = Inaccessible(throwable.getMessage)
  }
  case object Unset extends EnvVarResult {
    override val toString: String = "<unset>"
  }
  case class Error(message: String) extends EnvVarResult {
    override val toString: String = s"<error: $message>"
  }
  object Error {
    def apply(throwable: Throwable): Error = Error(throwable.getMessage)
  }
}
