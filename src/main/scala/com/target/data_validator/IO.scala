package com.target.data_validator

import java.io.{ByteArrayInputStream, File, FileWriter, IOException}
import java.net.URI

import com.typesafe.scalalogging.LazyLogging
import io.circe.Json
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.ipc.RemoteException
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.sys.process._
import scala.util.{Failure, Success, Try}
import scalatags.Text.all.Tag

object IO extends LazyLogging {

  // Handy constants
  val NEW_LINE = "\n"
  val FILE_SCHEMA_PREFIX = "file://"
  val HDFS_SCHEMA_PREFIX = "hdfs://"

  /** Helper for calling right function for local or hdfs files.
    * @param localFunc
    *   \- If filename is local call this function.
    * @param hdfsFunc
    *   \- If filename is HDFS call this function
    * @param default
    *   \- default to return when exception is thrown.
    * @return
    *   depends on semantics of localFunc/hdfsFunc.
    */
  private def localOrHdfs(
      filename: String,
      localFunc: String => Boolean,
      hdfsFunc: String => Boolean,
      default: Boolean = false
  )(implicit spark: SparkSession): Boolean = {
    if (filename.startsWith(HDFS_SCHEMA_PREFIX)) {
      hdfsFunc(filename)
    } else if (filename.startsWith(FILE_SCHEMA_PREFIX)) {
      localFunc(filename.stripPrefix(FILE_SCHEMA_PREFIX))
    } else {
      try {
        localFunc(filename)
      } catch {
        case re: RemoteException => logger.warn(s"Caught Exception, $re"); default
      }
    }
  }

  private def actionCanWrite(action: FsAction): Boolean = action.implies(FsAction.WRITE)

  private def pathCanWrite(fs: FileSystem, path: Path): Boolean = {
    val fileStatus = fs.getFileStatus(path)
    val perms = fileStatus.getPermission
    val currentUser = UserGroupInformation.getCurrentUser

    if (fileStatus.getOwner == currentUser.getShortUserName) {
      actionCanWrite(perms.getUserAction)
    } else {
      val userGroups = currentUser.getGroupNames.toList
      if (userGroups.contains(fileStatus.getGroup)) {
        actionCanWrite(perms.getGroupAction)
      } else {
        actionCanWrite(perms.getOtherAction)
      }
    }
  }

  private def parentIsWritable(fs: FileSystem, path: Path): Boolean = Option(path.getParent) match {
    case Some(parent) =>
      if (fs.exists(parent)) {
        pathCanWrite(fs, parent)
      } else {
        parentIsWritable(fs, parent)
      }
    case None => logger.warn("getParent returned NULL"); false
  }

  private def canAppendOrCreateHDFS(filename: String, append: Boolean)(implicit spark: SparkSession): Boolean = {
    Try(FileSystem.get(URI.create(filename), spark.sparkContext.hadoopConfiguration)) match {

      case Failure(e) =>
        logger.error(s"Could Not get HDFS filesystem for '$filename' message: ${e.getMessage}")
        false

      case Success(fs) =>
        val path = new Path(filename)

        if (fs.exists(path)) {
          if (append) {
            // Append mode, file must be writable.
            pathCanWrite(fs, path)
          } else {
            // We are overwriting, parent must be writable.
            parentIsWritable(fs, path)
          }
        } else {
          // File doesn't exist, parent must be writable.
          parentIsWritable(fs, path)
        }
    }
  }

  private def canAppendOrCreateLocal(filename: String, append: Boolean): Boolean = {
    val f = new File(filename)
    if (f.exists() && append) {
      f.canWrite
    } else {
      // Check if we can write to parent directory.
      val absolutePath = new File(f.getAbsolutePath)
      Option(absolutePath.getParentFile).exists(parent => parent.canWrite && parent.isDirectory)
    }
  }

  private def canExecuteLocal(filename: String): Boolean = {
    val f = new java.io.File(filename)
    f.canExecute
  }

  /** Checks a file to see if it can be appended to or created if it doesn't exist.
    *
    * @param append
    *   \- true if we can append to file.
    * @param spark
    *   \- SparkSession.
    * @return
    *   true - should be able to create or append.
    */
  def canAppendOrCreate(filename: String, append: Boolean)(implicit spark: SparkSession): Boolean =
    localOrHdfs(
      filename,
      (f: String) => canAppendOrCreateLocal(f, append),
      (f: String) => canAppendOrCreateHDFS(f, append)
    )

  /** Checks a filename to see if its Executable.
    *
    * @return
    *   true if executable.
    */
  def canExecute(filename: String)(implicit spark: SparkSession): Boolean = {
    localOrHdfs(filename, canExecuteLocal, (_: String) => false)
  }

  private def existsLocal(filename: String): Boolean = {
    try {
      val file = new File(filename)
      file.exists()
    } catch {
      case ioe: IOException =>
        logger.warn(s"Exception caught '$filename' ioe: $ioe")
        false
    }
  }

  private def existsHDFS(filename: String)(implicit spark: SparkSession): Boolean = {
    val fs = FileSystem.get(URI.create(filename), spark.sparkContext.hadoopConfiguration)
    fs.exists(new Path(filename))
  }

  /** Checks for existence of file
    * @param filename
    *   \- file to check for existence.
    * @param spark
    *   \- SparkSession
    * @return
    *   true if file exists
    */
  def exists(filename: String)(implicit spark: SparkSession): Boolean = {
    localOrHdfs(filename, existsLocal, existsHDFS)
  }

  /** Writes a string to local or HDFS
    *
    * @param filename
    *   \- Where to write str.
    * @param str
    *   \- String to write to filename
    * @param append
    *   \- True = append
    * @return
    *   true on error
    */
  def writeString(filename: String, str: String, append: Boolean = false)(implicit spark: SparkSession): Boolean = {
    localOrHdfs(
      filename,
      (f: String) => writeStringLocal(f, str, append),
      (f: String) => writeStringToHdfs(f, str, append)
    )
  }

  /** Writes a string to a local filesystem.
    *
    * @param filename
    *   \- Where to write str, if string doesn't end in new line, one will be appended.
    * @param str
    *   \- String to write to filename
    * @param append
    *   \- True = append
    * @return
    *   true on error
    */
  def writeStringLocal(filename: String, str: String, append: Boolean): Boolean = {
    try {
      val fw = new FileWriter(filename, append)
      fw.write(str)
      if (!str.endsWith(NEW_LINE)) {
        fw.write(NEW_LINE)
      }
      fw.close()
      false
    } catch {
      case io: IOException =>
        logger.error(s"Writing file '$filename' append: $append failed, ${io.getMessage}"); true
    }
  }

  /** Writes HTML to filename
    *
    * @param filename
    *   \- where to write HTML
    * @param html
    *   \- HTML to write to filename
    * @return
    *   true on error
    */
  def writeHTML(filename: String, html: Tag)(implicit spark: SparkSession): Boolean =
    writeString(filename, html.render)

  /** Writes JSON to filename
    *
    * @param filename
    *   \- Where to write JSON
    * @param json
    *   \- A JSON struct that will be written with noSpaces
    * @param append
    *   \- True = append
    * @return
    *   true on error
    */
  def writeJSON(filename: String, json: Json, append: Boolean = false)(implicit spark: SparkSession): Boolean =
    writeString(filename, json.noSpaces, append)

  /** Writes a String to HDFS path. If file exists, it will be overwritten. Returns true on error
    *
    * @param filename
    *   \- HDFS Path
    * @param str
    *   \- String to write to filename
    * @param append
    *   \- True = append
    * @param spark
    *   \- Spark session to get hdfs conf
    * @return
    *   true on failure
    */
  def writeStringToHdfs(
      filename: String,
      str: String,
      append: Boolean = false
  )(implicit spark: SparkSession): Boolean = {
    try {
      val fs = FileSystem.get(URI.create(filename), spark.sparkContext.hadoopConfiguration)
      val path = new Path(filename)
      val outputStream = if (append && fs.exists(path)) {
        fs.append(path)
      } else {
        fs.create(path, true)
      }

      outputStream.writeChars(str)
      if (!str.endsWith(NEW_LINE)) {
        outputStream.writeChars(NEW_LINE)
      }
      outputStream.close()
      false
    } catch {
      case io: IOException =>
        logger.error(s"Writing HDFS file '$filename' append: $append failed, ${io.getMessage}")
        true
    }
  }

  /** @param program
    *   \- program to pipe str to.
    * @param str
    *   \- str to send to program.
    * @return
    *   (fail: Boolean, stdout: String, stderr: String)
    */
  def writeStringToPipe(program: String, str: String): (Boolean, Seq[String], Seq[String]) = {

    try {
      val bis = new ByteArrayInputStream(str.getBytes)
      val (out, err) = (new ListBuffer[String], new ListBuffer[String])
      val pLog = ProcessLogger(out.append(_), err.append(_))

      logger.info(s"Sending ${str.length} of input to `$program`")
      val exitValue = Seq("sh", "-c", program) #< bis ! pLog
      logger.debug(s"exitValue: $exitValue")
      if (logger.underlying.isDebugEnabled()) {
        out.foreach(o => logger.debug(s"stdout: $o"))
        err.foreach(e => logger.debug(s"stderr: $e"))
      }
      if (exitValue != 0) {
        logger.error(s"Program '$program' returned $exitValue")
      }
      (exitValue != 0, out, err)
    } catch {
      case io: IOException =>
        logger.error(s"Writing to pipe, program: $program failed, ${io.getMessage}")
        (true, List.empty, List(io.getMessage))
      case rte: RuntimeException =>
        logger.error(s"Writing to pipe, program: $program failed, ${rte.getMessage}")
        (true, List.empty, List(rte.getMessage))
    }
  }

}
