package com.target.data_validator

import com.target.data_validator.validator._
import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.syntax._

import scala.collection.mutable.ListBuffer

object JsonEncoders extends LazyLogging {

  // Used by ValidatorQuickCheckError to make sure Json types are right.
  private def any2json(a: Any): Json = a match {
    case i: Int => i.asJson
    case l: Long => l.asJson
    case f: Float => f.asJson
    case d: Double => d.asJson
    case s: String => Json.fromString(s)
    case b: Boolean => b.asJson
    case a: Any => logger.warn(s"Unknown type `${a.getClass.getCanonicalName}` defaulting to string.")
      Json.fromString(a.toString)
  }

  implicit val eventEncoder: Encoder[ValidatorEvent] = new Encoder[ValidatorEvent] {
    override def apply(a: ValidatorEvent): Json = a match {
      case vc: ValidatorCounter => Json.obj(
        ("type", Json.fromString("counter")),
        ("name", Json.fromString(vc.name)),
        ("value", Json.fromLong(vc.value))
      )
      case vg: ValidatorGood => Json.obj(
        ("type", Json.fromString("good")),
        ("msg", Json.fromString(vg.msg))
      )
      case ve: ValidatorError => Json.obj(
        ("type", Json.fromString("error")),
        ("failed", Json.fromBoolean(ve.failed)),
        ("msg", Json.fromString(ve.msg))
      )
      case vt: ValidatorTimer => Json.obj(
        ("type", Json.fromString("timer")),
        ("label", Json.fromString(vt.label)),
        ("ns", Json.fromLong(vt.duration))
      )
      case vce: ValidatorCheckEvent => Json.obj(
        ("type", Json.fromString("checkEvent")),
        ("failed", Json.fromBoolean(vce.failed)),
        ("label", Json.fromString(vce.label)),
        ("count", Json.fromLong(vce.count)),
        ("errorCount", Json.fromLong(vce.errorCount))
      )
      case cbvce: ColumnBasedValidatorCheckEvent =>
        val fields = new ListBuffer[Tuple2[String, Json]]
        fields.append(("type", Json.fromString("columnBasedCheckEvent")))
        fields.append(("failed", Json.fromBoolean(cbvce.failed)))
        fields.append(("message", Json.fromString(cbvce.msg)))
        cbvce.data.foreach(x => fields.append((x._1, Json.fromString(x._2))))
        Json.fromFields(fields)

      case qce: ValidatorQuickCheckError => Json.obj(
        ("type", Json.fromString("quickCheckError")),
        ("failed", Json.fromBoolean(qce.failed)),
        ("message", Json.fromString(qce.message)),
        ("key", Json.fromFields(qce.key.map(x => (x._1, any2json(x._2)))))
      )
      case vs: VarSubEvent => Json.obj(
        ("type", Json.fromString("variableSubstitution")),
        ("src", Json.fromString(vs.src)),
        ("dest", Json.fromString(vs.dest))
      )
      case vs: VarSubJsonEvent => Json.obj(
        ("type", Json.fromString("variableSubstitution")),
        ("src", Json.fromString(vs.src)),
        ("dest", vs.dest)
      )
    }
  }

  implicit val baseEncoder: Encoder[ValidatorBase] = new Encoder[ValidatorBase] {
    final def apply(a: ValidatorBase): Json = a.toJson
  }

  implicit val tableEncoder: Encoder[ValidatorTable] = new Encoder[ValidatorTable] {
    override final def apply(a: ValidatorTable): Json = a match {
      case vh: ValidatorHiveTable => Json.obj(
        ("db", Json.fromString(vh.db)),
        ("table", Json.fromString(vh.table)),
        ("failed", vh.failed.asJson),
        ("keyColumns", vh.keyColumns.asJson),
        ("checks", vh.checks.asJson),
        ("events", vh.getEvents.asJson)
      )
      case vo: ValidatorOrcFile => Json.obj(
        ("orcFile", Json.fromString(vo.orcFile)),
        ("failed", vo.failed.asJson),
        ("keyColumns", vo.keyColumns.asJson),
        ("checks", vo.checks.asJson),
        ("events", vo.getEvents.asJson))
      case vp: ValidatorParquetFile => Json.obj(
        ("parquetFile", Json.fromString(vp.parquetFile)),
        ("failed", vp.failed.asJson),
        ("keyColumns", vp.keyColumns.asJson),
        ("checks", vp.checks.asJson),
        ("events", vp.getEvents.asJson))
    }
  }

  implicit val configVarEncoder: Encoder[ConfigVar] = new Encoder[ConfigVar] {
    override def apply(a: ConfigVar): Json = a match {
      case nv: NameValue => Json.obj(
        ("name", Json.fromString(nv.name)),
        ("value", nv.value)
      )
      case ne: NameEnv => Json.obj(
        ("name", Json.fromString(ne.name)),
        ("env", Json.fromString(ne.env))
      )
      case nshell: NameShell => Json.obj(
        ("name", Json.fromString(nshell.name)),
        ("shell", Json.fromString(nshell.shell))
      )
      case nsql: NameSql => Json.obj(
        ("name", Json.fromString(nsql.name)),
        ("shell", Json.fromString(nsql.sql))
      )
      case x =>
        logger.error(s"Unknown configVar type: $x")
        throw new RuntimeException(s"Unknown configVar type: $x")
    }
  }

  implicit val configOutputEncoder: Encoder[ValidatorOutput] = new Encoder[ValidatorOutput] {
    override def apply(a: ValidatorOutput): Json = a match {

      case file: FileOutput => Json.obj(
        ("filename", Json.fromString(file.filename)),
        ("append", Json.fromBoolean(file.append.getOrElse(false)))
      )
      case pipe: PipeOutput => Json.obj(
        ("pipe", Json.fromString(pipe.pipe)),
        ("ignoreError", Json.fromBoolean(pipe.ignoreError.getOrElse(false)))
      )
      case x =>
        logger.error(s"Unknown output type: $x")
        throw new RuntimeException(s"Unknown output type: $x")
    }
  }

}
