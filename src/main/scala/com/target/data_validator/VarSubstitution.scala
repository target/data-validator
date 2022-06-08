package com.target.data_validator

import com.typesafe.scalalogging.LazyLogging
import io.circe.Json

import scala.collection.mutable

// Helper Class to handle variable substitution and manage dict of (k,v)

class VarSubstitution() extends LazyLogging {
  import VarSubstitution._

  val dict = new mutable.HashMap[String, Json]()

  /** Adds (k,v) to dictionary.
    *
    * @param key
    *   \- key of value in dictionary.
    * @param value
    *   \- value of key in dictionary.
    * @return
    *   True on error.
    */
  def add(key: String, value: Json): Boolean = {
    if (VAR_REGEX.findFirstIn(key).isEmpty) {
      logger.error(s"Bad key: $key, must follow variable rules.")
      true
    } else if (value.asString.exists(VAR_BODY_REGEX.findFirstIn(_).isDefined)) {
      logger.error(s"Cannot have variable defined in value: $value!")
      true
    } else {
      if (dict.contains(key)) {
        logger.warn(s"Dict already contains key: '$key' current: '${dict(key)}' v: '$value' not overriding.")
        true
      } else {
        dict += (key -> value)
        false
      }
    }
  }

  /** Adds a String to dictionary.
    * @param value
    *   \- gets converted to Json
    * @return
    *   True on error
    */
  def addString(key: String, value: String): Boolean = {
    replaceVars(value) match {
      case Left(newValue) => add(key, JsonUtils.string2Json(newValue))
      case Right(_) => true // Bug: Ignoring the ValidatorError
    }
  }

  /** Removes key from dictionary.
    *
    * @param k
    *   \- key to be removed.
    * @return
    *   True on error.
    */
  def remove(k: String): Boolean = {
    if (dict.contains(k)) {
      dict.remove(k)
      false
    } else {
      logger.warn(s"remove(k:$k) Dict doesn't contain specified key.")
      true
    }
  }

  /** replaces variables in String.
    *
    * @param s
    *   \- string to replace variables in.
    * @return
    *   Left(new string) on Success, Right(ValidatorEvent) on Error
    */
  def replaceVars(s: String): Either[String, ValidatorEvent] = {
    val variableJson = findVars(s).toSeq.map(x => (x, getVarName(x).flatMap(dict.get)))
    val (foundVariableJson, missingVariableJson) = variableJson.partition(_._2.isDefined)
    foundVariableJson.foreach(x => logger.debug(s"foundVar: $x"))
    missingVariableJson.foreach(x => logger.debug(s"missingVar: $x"))

    val newString = foundVariableJson.foldRight(s) { (vj, ns) =>
      val (variable, json) = vj
      logger.debug(s"accum: $ns variable: $variable json: $json")
      val replacement = jsonToString(json.get)
      replaceAll(ns, variable, replacement)
    }

    val errs = missingVariableJson.map(x => x._1)
    errs.foreach(x => logger.debug(s"errs: $x"))
    if (errs.nonEmpty) {
      Right(
        ValidatorError(
          "VariableSubstitution: Can't find values for the following keys, " +
            s"${errs.flatMap(getVarName).mkString(",")}"
        )
      )
    } else {
      if (s != newString) {
        logger.debug(s"Replaced '$s' with '$newString'")
      }
      Left(newString)
    }
  }

  private def jsonToString(j: Json): String = {
    if (j.isString) {
      j.asString.get
    } else {
      j.toString()
    }
  }

  def replaceJsonVars(j: Json): Either[Json, ValidatorEvent] = {
    if (j.isString) {
      replaceVars(j.asString.get).left.map(JsonUtils.string2Json)
    } else {
      // Since variables are only in Strings, return j.
      Left(j)
    }
  }

  private def logDupKeys(k: String, v: String): Unit = {
    logger.info(s"Adding dict entry k: $k v:`$v`")
    if (dict.contains(k)) logger.warn(s"Replacing key: $k old: ${dict(k)} with new: $v")
  }

  /** Adds the map m to dict
    */
  def addMap(m: Map[String, String]): Unit = {
    val kj = m.map { case (k, v) =>
      logDupKeys(k, v)
      (k, JsonUtils.string2Json(v))
    }
    dict ++= kj
  }

  override def equals(obj: Any): Boolean =
    obj.isInstanceOf[VarSubstitution] && obj.asInstanceOf[VarSubstitution].dict == dict

  override def hashCode(): Int = dict.hashCode()
}

object VarSubstitution extends LazyLogging {
  private val VAR_REGEX_STR = "[A-Za-z][A-Za-z0-9_]*"
  private val VAR_REGEX = VAR_REGEX_STR.r
  private val VAR_BODY_REGEX = ("\\$" + VAR_REGEX_STR + "|\\$\\{" + VAR_REGEX_STR + "\\}").r

  def findVars(s: String): Set[String] = {
    VAR_BODY_REGEX.findAllIn(s).toSet
  }

  /** Checks if s is a variable.
    * @param s
    *   \- string to check
    * @return
    *   true if s is a variable.
    */
  def isVariable(s: String): Boolean = s.startsWith("$") && VAR_BODY_REGEX.findFirstMatchIn(s).isDefined

  /** Replaces all the occurrences of oldVal in src with newVal.
    *
    * @param src
    *   \- source string that contains values to be replaced.
    * @param oldVal
    *   \- old Value that will be replaced by newValue.
    * @param newVal
    *   \- new Value that will replace oldValue.
    * @return
    *   new string with newVal were oldVal was.
    */
  def replaceAll(src: String, oldVal: String, newVal: String): String = {
    val buf = new StringBuffer(src)
    var idx = buf.indexOf(oldVal)
    while (idx >= 0) {
      buf.replace(idx, idx + oldVal.length, newVal)
      idx = buf.indexOf(oldVal, idx + newVal.length)
    }
    val ret = buf.toString
    logger.debug(s"src: $src oldVal: $oldVal newVal: $newVal ret: $ret")
    ret
  }

  /** gets the variable name from the variable. ie "$\{foo\}" returns "foo"
    *
    * @param rawVar
    *   \- variable with control chars.
    * @return
    *   variable without '$' or '{','}'
    */
  def getVarName(rawVar: String): Option[String] = {
    val ret = if (rawVar.startsWith("${")) {
      if (rawVar.endsWith("}")) {
        Some(rawVar.substring(2, rawVar.length - 1))
      } else {
        None
      }
    } else if (rawVar.startsWith("$")) {
      Some(rawVar.substring(1))
    } else {
      logger.error(s"Illegal Variable $rawVar")
      None
    }
    logger.debug(s"getVarName(K: $rawVar) ret: $ret")
    ret
  }
}
