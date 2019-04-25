package com.target.data_validator

import com.target.TestingSparkSession
import io.circe.{parser, Json}
import org.scalatest.{FunSpec, Matchers}

import scala.io.Source.fromFile
import scala.util.Random._
import scalatags.Text.all._

class IOSpec extends FunSpec with Matchers with TestingSparkSession {
  val SAMPLE_HTML = html(h1("H1"), "Sample HTML Doc")
  val SAMPLE_JSON: Json = parser
    .parse("""{
             | "one" : 1,
             | "two" : [ 1,2 ],
             | "three": { "a":1.0,"b":2.0,"c":3.0 }
             |}
           """.stripMargin).right.getOrElse(Json.Null)

  val tempDir: String = System.getProperty("java.io.tmpdir")

  def createRandomTempFilename: String = {
    tempDir + "%d.tmp".format(Math.abs(nextInt))
  }

  def rm(filename: String): Boolean = {
    val f = new java.io.File(filename)
    f.delete()
  }

  describe("Local Disk") {

    it("should write HTML") {
      val filename = createRandomTempFilename
      assert(!IO.writeHTML(filename, SAMPLE_HTML)(spark))
      fromFile(filename).mkString should be(SAMPLE_HTML.render + "\n")
      assert(rm(filename))
    }

    it("should write JSON") {
      val filename = createRandomTempFilename
      assert(!IO.writeJSON(filename, SAMPLE_JSON)(spark))
      fromFile(filename).mkString should be(SAMPLE_JSON.noSpaces + "\n")
      assert(rm(filename))
    }

    it("file:/// should be able to write") {
      val baseFilename = createRandomTempFilename
      val filename = "file://" + baseFilename
      val data = nextString(128) + IO.NEW_LINE //scalastyle:ignore
      assert(!IO.writeString(filename, data)(spark))
      fromFile(baseFilename).mkString should be(data)
      assert(rm(baseFilename))
    }

    describe("canAppendOrWrite") {

      it("returns false when it should") {
        val badFilename = "/dir/that/does/not/exist/junk.txt"
        assert(!IO.canAppendOrCreate(badFilename, append = false)(spark))
      }

      it("returns true when it should") {
        assert(IO.canAppendOrCreate(createRandomTempFilename, append = false)(spark))
      }

    }

    describe("canExecute") {

      it("returns true for local executable") {
        assert(IO.canExecute("/usr/bin/wc")(spark))
      }

      it("returns false for hdfs file") {
        assert(!IO.canExecute(IO.HDFS_SCHEMA_PREFIX + "foo.bar")(spark))
      }

      it("returns false for local non-executable") {
        assert(!IO.canExecute("/etc/passwd")(spark))
      }

      it("returns false for non-existent file") {
        assert(!IO.canExecute(createRandomTempFilename)(spark))
      }

    }

  }

  describe("writeStringToPipe") {

    it("Fails for bad path") {
      val (fail, out, _) = IO.writeStringToPipe("/bad/path", nextString(200)) // scalastyle:ignore
      assert(fail)
      assert(out.isEmpty)
    }

    it("Works for wc and captures stdout") {
      val str = nextString(200) // scalastyle:ignore
      val (fail, out, err) = IO.writeStringToPipe("/usr/bin/wc -c", str)
      assert(!fail)
      assert(out.length == 1)
      assert(out.head.dropWhile(_.isWhitespace).toInt == str.getBytes.length)
      assert(err.isEmpty)
    }

    it("works when program fails") {
      val (fail, out, err) = IO.writeStringToPipe("false", "")
      assert(fail)
      assert(out.isEmpty)
      assert(err.isEmpty)
    }

    it("captures stderr and doesn't fail") {
      val (fail, out, err) = IO.writeStringToPipe("echo ERR >&2", "")
      assert(!fail)
      assert(out.isEmpty)
      assert(err == List("ERR"))
    }

  }
  // TODO: Add hdfs tests using https://github.com/sakserv/hadoop-mini-clusters
}
