package com.target.data_validator

import scopt.OptionParser

case class CliOptions(
    configFilename: String = "",
    verbose: Boolean = false,
    jsonReport: Option[String] = None,
    htmlReport: Option[String] = None,
    exitErrorOnFail: Boolean = true,
    vars: Map[String, String] = Map(),
    emailOnPass: Boolean = false
)

object CliOptionParser {

  def parser: OptionParser[CliOptions] = new OptionParser[CliOptions]("data-validator") {
    head(BuildInfo.name, "v" + BuildInfo.version)

    version("version")

    opt[Unit]("verbose").action((_, c) => c.copy(verbose = true)).text("Print additional debug output.")

    opt[String]("config")
      .action((fn, c) => c.copy(configFilename = fn))
      .text(
        "required validator config .yaml filename, " +
          "prefix w/ 'classpath:' to load configuration from JVM classpath/resources, " +
          "ex. '--config classpath:/config.yaml'"
      )

    opt[String]("jsonReport").action((fn, c) => c.copy(jsonReport = Some(fn))).text("optional JSON report filename")

    opt[String]("htmlReport").action((fn, c) => c.copy(htmlReport = Some(fn))).text("optional HTML report filename")

    opt[Map[String, String]]("vars")
      .valueName("k1=v1,k2=v2...")
      .action((x, c) => c.copy(vars = x))
      .text("other arguments")

    opt[Boolean]("exitErrorOnFail")
      .valueName("true|false")
      .action((x, c) => c.copy(exitErrorOnFail = x))
      .text(
        "optional when true, if validator fails, call System.exit(-1) " +
          "Defaults to True, but will change to False in future version."
      )

    opt[Boolean]("emailOnPass")
      .valueName("true|false")
      .action((x, c) => c.copy(emailOnPass = x))
      .text("optional when true, sends email on validation success. Default: false")

    help("help").text("Show this help message and exit.")
  }
}
