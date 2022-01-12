# data-validator [![Build Status](https://travis-ci.org/target/data-validator.svg?branch=master)](https://travis-ci.org/target/data-validator) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A tool to validate data in HIVE tables.

## Usage

Assemble fat jar: `sbt clean assembly`

```
spark-submit --master local data-validator-assembly-0.13.0.jar --help

data-validator v0.13.0
Usage: data-validator [options]

  --version
  --verbose                Print additional debug output.
  --config <value>         required validator config .yaml filename, prefix w/ 'classpath:' to load configuration from JVM classpath/resources, ex. '--config classpath:/config.yaml'
  --jsonReport <value>     optional JSON report filename
  --htmlReport <value>     optional HTML report filename
  --vars k1=v1,k2=v2...    other arguments
  --exitErrorOnFail true|false
                           optional when true, if validator fails, call System.exit(-1) Defaults to True, but will change to False in future version.
  --emailOnPass true|false
                           optional when true, sends email on validation success. Default: false
  --help                   Show this help message and exit.
```

### Example Run

```sh
spark-submit \
  --num-executors 10 \
  --executor-cores 2 \
  data-validator-assembly-0.13.0.jar \
  --config config.yaml \
  --jsonReport report.json
```

See the [Example Config](#example-config) below for the contents of `config.yaml`.

## Config file Description

The data-validator config file is yaml based and it has 3 sections,
Global Settings, Table Sources, and Validators.  The Table Sources,
and Validators have the ability to use variables in the
configuration. These variables are replaced at runtime with the values
set via `Global Settings` section or the `--vars` option on the
command line.  Variables start with `$` and must contain a word
starting with a letter (A-Za-z) and followed by zero or more letters
(A-Za-z), numbers(0-9), or underscore. Variables can optionally be
wrapped in `{` `}`. i.e. `$foo`, `${foo}` See the
[code](src/main/scala/com/target/data_validator/VarSubstitution.scala#L141)
for the regular expression used to find them in a string. All the
table sources, and all but one validator (`rowCount`) supports
variables in their configuration parameters. **Note:** Care must be taken
for some of the substitutions, some possible values might require
quoting the variables in the config.

### Global Settings

The first section is the global settings that are used
throughout the program.

| Variable | Type | Required | Description
|:---|:---|:---|:---:
| `numKeyCols` | Int | Yes | The number of columns from the table schema to use to uniquely identify a row in the table.
| `numErrorsToReport` | Int | Yes | The number of detailed errors to include in Validator Report.
| `detailedErrors` | Boolean | Yes | If a check fails, run a second pass and gather `numErrorToReport` examples of failure.
| `email` |EmailConfig|No| See [Email Config](#email-config).
| `vars` | Map | No | A map of (key, value) pairs used for variable substitution in `tables` config. See next section.
| `outputs`| Array | No | Describes where to send `.json` report. See [Validator Output](#validator-output).
| `tables` | List | Yes | List of table sources used to load tables to validate.

#### Email Config

| Variable | Type | Required | Description |
|:---|:---|:---|:---:
| `smtpHost` | String | Yes | The smtp host to send email message through.
| `subject` | String | Yes | Subject for email message.
| `from` | String | Yes | Email address to appear in from part of message.
| `to` | Array[String] | Yes | Must specify at least one email address to send the email report to.
| `cc` | Array[String] | No | Optional list of email addresses to send message to via `cc` field in message.
| `bcc` | Array[String] | No | Optional list of email addresses to send message to via `bcc` field in message.

Note that Data Validator only sends email on _failure_ by default. To send email even on successful runs,
pass `--emailOnPass true` to the command line.

#### Defining Variables

There are 4 different types of variables that you can specify, simple, environment, shell and SQL.

##### Simple Variable

Simple variables are specified by the `name` and `value` pairs and are very straight forward.

```yaml
vars:
  - name: ENV
    value: prod
```

This sets the variable `ENV` to the value `prod`

##### Environment Variable

Environment variables import the value from the [operating system](https://docs.oracle.com/javase/tutorial/essential/environment/env.html)

```yaml
vars:
  - name: JAVA_DIR
    env: JAVA_HOME
```

This will set the variable `JAVA_DIR` to the value returned by the `System.getenv("JAVA_HOME")`
If `JAVA_HOME` does not exist in the system environment, the data-validator will stop processing and exit with an error.

##### Shell Variable

Shell variable will take the first line of output from a shell command and store it a variable.

```yaml
vars:
  - name: NEXT_SATURDAY
    shell: date -d "next saturday" +"%Y-%m-%d"
```

This will set the variable `NEXT_SATURDAY` to the first line of output from the shell command `date -d "next saturday" +"%Y-%m-%d"`.

##### SQL Variable

SQL variable will take the first column from the first row of the results from a Spark SQL statement.

```yaml
vars:
  - name: MAX_AGE
    sql: select max(age) from census_income.adult
```

This runs the sql command that gets the max value from the column `age` from the table `adult` in the `census_income` database and stores it in `MAX_AGE`.

### Validator Output

In addition to the `--jsonReport` command line option, the `.yaml` has a `outputs` section that directs the .json event report to a file or pipes it to a program. There is no current limit on the number of outputs.

#### Filename

```yaml
outputs:
  - filename: /user/home/sample.json
    append: true
```

If the `filename` specified begins with a `/` or `local:///` it is written to the local filesystem. If the filename begins with `hdfs://` the report is written to the hdfs path. An optional `append` boolean can be specified, and if it is `true` the current report will be appended to the end of the specified file. The default is `append: false` and the filename is overwritten. The `filename` supports variable substitution, the optional `append` does not. Before the validator starts processing tables, it checks to verify that it can create or append to the `filename`, if it cannot, the data validator will exit with an error (non-zero value).

#### Pipe

```yaml
outputs:
  - pipe: /path/to/program
    ignoreError: true
```

A `pipe` is used to send the `.json` event report to another program for processing. This is a very powerful feature, and can enable the data-validator to be integrated with virtually any other system. An optional `ignoreError` boolean can also be specified, if `true` the exit value of the program will be ignored. If `false` (default) and the program exits with a non-zero status, the data-validator will fail.  The `pipe` supports variable substitution, the optional `ignoreError` does not.

Before the validator starts processing tables, it checks to see if the `pipe` program is executable, if it is not, the data-validator will exit with an error (non-zero value). The program must be on a local filesystem to be executed.

### Table Sources

Table sources are used to specify how to load the tables to be
validated. Currently supported sources are HiveTable, and
OrcFile. Each table source has 3 common arguments, `keyColumns`, `condition`,
`checks`, and its own source specific argument(s). The `keyColumns`
are list of columns that can be used to uniquely identify a row in the
table for the detailed error report when a validator fails. The `condition`
enables the user to specify a snippet of sql to pass to the where clause.
  The `checks` argument is a list of validators to run on this table.

#### HiveTable

To validate a Hive table, specify the `db` and the `table`, see below.

```yaml
- db: $DB
  table: table_name
  condition: "col1 < 100"
  keyColumns:
    - col1
    - col2
  checks:
```

#### OrcFile

To validate an `.orc` file, specify `orcFile` and the path to the file, see below.

```yaml
- orcFile: /path/to/orc/file
  keyColumns:
    - col1
    - col2
  checks:
```

#### Parquet File

To validate an `.parquet` file, specify `parquetFile` and the path to the file, see below.

```yaml
- parquetFile: /path/to/parquet/file
  keyColumns:
    - col1
    - col2
  checks:
```

### Validators

  The third section are the validators. To specify a validator, you
first specify the type as one of the validators, then specify the
arguments for that validator. Some of the validators support an error
threshold. This options allows the user to specify the number of errors
or percentage of errors they can tolerate.  In some use cases, it
might not be possible to eliminate all errors in the data.

##### Thresholds

Thresholds can be specified as an absolute number of errors, or a percentage of the row count.
If the threshold is `>= 1` it is considered an absolute number of errors. For example `1000` would fail the check if there are more then 1000 rows that failed the check.

If the threshold is `< 1` it is considered a fraction of the row count. For example `0.25` would fail the check if more then `rowCount * 0.25` of the rows fail the check.
If the threshold ends in a `%` its considered a percentage of the row count. For eample `33%` would fail the check if more then `rowCount * 0.33` of the rows fail the check.

Currently supported validators are listed below:

#### `columnMaxCheck`

Takes 2 parameters, the column name and a `value`. The check will fail if `max(column)` is **not equal** to the value.

| Arg | Type | Description |
|-----|------|-------------|
| `column` | String | Column within table to find the max from.
| `value`  | \*     | The column max should equal this value or the check will fail.  **Note:** The type of the value should match the type of the column. If the column is a `NumericType`, the value cannot be a `String`.

#### `negativeCheck`

Takes a single parameter, the column name to check. The validator will fail if any rows with that column are negative.

| Arg | Type | Description |
|-----|------|-------------|
| `column` | String | Table column to be checked for negative values.  If it contains a `null` validator will fail.  **Note:** Column must be of a `NumericType` or the check will fail during the config check.
| `threshold` | String | See above description of threshold.

#### `nullCheck`

Takes a single parameter, the column name to check. The validator will fail if any rows with that column are `null`.

| Arg | Type | Description |
|-----|------|-------------|
| `column` | String | Table column to be checked for `null`.  If it contains a `null` validator will fail.
| `threshold` | String | See above description of threshold.

#### `rangeCheck`

Takes 2 - 4 parameters, described below. If the value in the column doesn't fall within the range specified by (`minValue`, `maxValue`) the check will fail.

| Arg | Type | Description |
|-----|------|-------------|
| `column` | String | Table column to be checked.
| `minValue` | \* | lower bound of the range, or other column in table. Type depends on the type of the `column`.
| `maxValue` | \* | upper bound of the range, or other column in table. Type depends on the type of the `column`.
| `inclusive` | Boolean | Include `minValue` and `maxValue` as part of the range.
| `threshold` | String | See above description of threshold.

**Note:** To specify another column in the table, you must prefix the column name with a **`** (backtick).

#### `stringLengthCheck`

Takes 2 to 4 parameters, described in the table below. If the length of the string in the column doesn't fall within the range specified by (`minLength`, `maxLength`), both inclusive, the check will fail.
At least one of `minLength` or `maxLength` must be specified. The data type of `column` must be String.

| Arg | Type | Description |
|-----|------|-------------|
| `column` | String | Table column to be checked. The DataType of the column must be a String
| `minLength` | Integer | Lower bound of the length of the string, inclusive.
| `maxLength` | Integer | Upper bound of the length of the string, inclusive.
| `threshold` | String | See above description of threshold.

#### `stringRegexCheck`

Takes 2 to 3 parameters, described in the table below. If the `column` value does not match the pattern specified by the `regex`, the check will fail.
A value for `regex` must be specified. The data type of `column` must be String.

| Arg         | Type   | Description                                                             |
|-------------|--------|-------------------------------------------------------------------------|
| `column`    | String | Table column to be checked. The DataType of the column must be a String |
| `regex`     | String | POSIX regex.                                                            |
| `threshold` | String | See above description of threshold.                                     |

#### `rowCount`

The minimum number of rows a table must have to pass the validator.

| Arg | Type | Description |
|-----|------|-------------|
| `minNumRows` | Long | The minimum number of rows a table must have to pass.

See Example Config file below to see how the checks are configured.

#### `uniqueCheck`

This check is used to make sure all rows in the table are unique, only the columns specified are used to determine uniqueness.
This is a costly check and requires an additional pass through the table.

| Arg | Type | Description |
|-----|------|-------------|
| `columns` | Array[String] | Each set of values in these columns must be unique.

#### `columnSumCheck`

This check sums a column in all rows. If the sum applied to the `column` doesn't fall within the range specified by (`minValue`, `maxValue`) the check will fail.

| Arg         | Type        | Description                                                            |
|-------------|-------------|------------------------------------------------------------------------|
| `column`    | String      | The column to be checked.                                              |
| `minValue`  | NumericType | The lower bound of the sum.  Type depends on the type of the `column`. |
| `maxValue`  | NumericType | The upper bound of the sum. Type depends on the type of the `column`.  |
| `inclusive` | Boolean     | Include `minValue` and `maxValue` as part of the range.                |

**Note:** If bounds are non-inclusive, and the actual sum is equal to one of the bounds, the relative error percentage will be undefined.

#### `colstats`

This check generates column statistics about the specified column.

| Arg         | Type        | Description                                |
|-------------|-------------|--------------------------------------------|
| `column`    | String      | The column on which to collect statistics. |

These keys and their corresponding values will appear in the check's JSON summary when using the JSON report output mode:

| Key         | Type        | Description                                                                                                             |
|-------------|-------------|-------------------------------------------------------------------------------------------------------------------------|
| `count`     | Integer     | Count of non-null entries in the `column`.                                                                              |
| `mean`      | Double      | Mean/Average of the values in the `column`.                                                                             |
| `min`       | Double      | Smallest value in the `column`.                                                                                         |
| `max`       | Double      | Largest value in the `column`.                                                                                          |
| `stdDev`    | Double      | Standard deviation of the values in the `column`.                                                                       |
| `histogram` | Complex     | Summary of an equi-width histogram, counts of values appearing in 10 equally sized buckets over the range `[min, max]`. |

## Example Config

```yaml
---

# If keyColumns are not specified for a table, we take the first N columns of a table instead.
numKeyCols: 2

# numErrorsToReport: Number of errors per check show in "Error Details" of report, this is to limit the size of the email.
numErrorsToReport: 5

# detailedErrors: If true, a second pass will be made for checks that fail to gather numErrorsToReport examples with offending value and keyColumns to aide in debugging
detailedErrors: true

vars:
  - name: ENV
    value: prod

  - name: JAVA_DIR
    env: JAVA_HOME

  - name: TODAY
    shell: date + "%Y-%m-%d"

  - name: MAX_AGE
    sql: SELECT max(age) FROM census_income.adult

outputs:
  - filename: /user/home/sample.json
    append: true

  - pipe: /path/to/program
    ignoreError: true

email:
  smtpHost: smtp.example.com
  subject: Data Validation Summary
  from: data-validator-no-reply@example.com
  to:
    - person1@example.com
  cc:
    - person2@example.com, person3@example.com
  bcc:
    - person4@example.com

tables:
  - db: census_income
    table: adult
    # Key Columns are used when errors occur to identify a row, so they should include enough columns to uniquely identify a row.
    keyColumns:
      - age
      - occupation
    condition: educationNum >= 5
    checks:
      # rowCount - checks if the number of rows is at least minRows
      - type: rowCount
        minNumRows: 50000

      # negativeCheck - checks if any values are less than 0
      - type: negativeCheck
        column: age
      
      # colstats - adds basic statistics of the column to the output
      - type: colstats
        column: age
        
      # nullCheck - checks if the column is null, counts number of rows with null for this column.
      - type: nullCheck
        column: occupation

      # stringLengthCheck - checks if the length of the string in the column falls within the specified range, counts number of rows in which the length of the string is outside the specified range.
      - type: stringLengthCheck
        column: occupation
        minLength: 1
        maxLength: 5

      # stringRegexCheck - checks if the string in the column matches the pattern specified by `regex`, counts number of rows in which there is a mismatch.
      - type: stringRegexCheck
        column: occupation
        regex: ^ENGINEER$ # matches the word ENGINEER

      - type: stringRegexCheck
        column: occupation
        regex: \w # matches any alphanumeric string
```

## Working with OOZIE Workflows

The data-validator can be used in an oozie workflow to halt the wf if a check doesn't pass. There are 2 ways to use the data-validator in oozie and each has their own drawbacks. The selection of the methods is determined by the `--exitErrorOnFail {true|false}` command line option.

### Setting ExitErrorOnFail to True

The first option, enabled by `--exitErrorOnFail=true`, is to  have the data-validator exit with a non-zero value when a check fails. This enables the workflow to decide how it wants to handle a failed check/error.  The downsides of this method, is that you can never be sure if the data-validator exited with an error because bad check, or if there was a problem with the execution of the data-validator. This also pollutes the oozie workflow info with `ERROR`, which some might not like. This is currently the default but likely to change with `v1.0.0`.

Example oozie wf snippet:

```xml
<action name="RunDataValidator">
    <shell xmlns="uri:oozie:shell-action:0.2">
      <job-tracker>${jobTracker}</job-tracker>
      <name-node>${nameNode}</name-node>
      <exec>spark-submit</exec>
      <argument>--conf</argument>
      <argument>spark.yarn.maxAppAttempts=1</argument>
      <argument>--class</argument>
      <argument>com.target.data_validator.Main</argument>
      <argument>--master</argument>
      <argument>yarn</argument>
      <argument>--deploy-mode</argument>
      <argument>cluster</argument>
      <argument>--keytab</argument>
      <argument>${keytab}</argument>
      <argument>--principal</argument>
      <argument>${principal}</argument>
      <argument>--files</argument>
      <argument>config.yaml</argument>
      <argument>data-validator-assembly-0.13.0.jar</argument>
      <argument>--config</argument>
      <argument>config.yaml</argument>
      <argument>--exitErrorOnFail</argument>
      <argument>true</argument>
      <argument>--vars</argument>
      <argument>ENV=${ENV},EMAIL_REPORT=${EMAIL_REPORT},SMTP_HOST=${SMTP_HOST}</argument>
      <capture-output/>
    </shell>
    <ok to="ValidatorSuccess" />
    <error to="ValidatorErrorOrCheckFail" />
  </action>

 <action name="ValidatorErrorOrCheckFail">
  <!-- Check or data-validator failed  -->
  </action>

  <action name="ValidatorSuccess">
  <!-- Everything is wonderful!  -->
  </action>
```


### Setting ExitErrorOnFail to False

The second option, enabled by `--exitErrorOnFail=false`, is to have the data-validator output to stdout `DATA_VALIDATOR_STATUS=PASS` or `DATA_VALIDATOR_STATUS=FAIL` and `System.exit(0)` when it completes. This enables the workflow to distinguish between a failed check, and a runtime error.
The downside is that you must use the oozie shell action,  with the capture output option, and run the validator via Spark's client mode. This will likely become the default behavior in `v1.0.0`.

Example oozie wf snippet:

```xml
<action name="RunDataValidator">
  <shell xmlns="uri:oozie:shell-action:0.2">
    <job-tracker>${jobTracker}</job-tracker>
    <name-node>${nameNode}</name-node>
    <exec>spark-submit</exec>
    <argument>--conf</argument>
    <argument>spark.yarn.maxAppAttempts=1</argument>
    <argument>--class</argument>
    <argument>com.target.data_validator.Main</argument>
    <argument>--master</argument>
    <argument>yarn</argument>
    <argument>--deploy-mode</argument>
    <argument>client</argument>
    <argument>--keytab</argument>
    <argument>${keytab}</argument>
    <argument>--principal</argument>
    <argument>${principal}</argument>
    <argument>data-validator-assembly-0.13.0.jar</argument>
    <argument>--config</argument>
    <argument>config.yaml</argument>
    <argument>--exitErrorOnFail</argument>
    <argument>false</argument>
    <argument>--vars</argument>
    <argument>ENV=${ENV},EMAIL_REPORT=${EMAIL_REPORT},SMTP_HOST=${SMTP_HOST}</argument>
    <capture-output/>
  </shell>
  <ok to="ValidatorDecision" />
  <error to="VaildatorError" />
</action>

<decision name="ValidatorDecision">
  <switch>
    <case to="ValidatorCheckFail">${wf:actionData('RunDataValidator')['DATA_VALIDATOR_STATUS'] eq "FAIL"}</case>
    <case to="ValidatorCheckPass">${wf:actionData('RunDataValidator')['DATA_VALIDATOR_STATUS'] eq "PASS"}</case>
    <default to="ValidatorNeither"/>
  </switch>
</decision>

<action name="ValidatorCheckFail">
  <!-- Handle Failed Check -->
</action>

<action name="ValidatorCheckPass">
  <!-- Everything is Wonderful! -->
</action>

<action name="ValidatorFailure">
  <!-- Notify devs of validator failure -->
</action>
```

## Development Tools

### Generate testing data with GenTestData or `sbt generateTestData` 

Data Validator includes a tool to generate a sample `.orc` file for use in local development.
This repo's SBT configuration wraps the tool in a convenient SBT task: `sbt generateTestData`  
If you run this program or task, it will generate a file `testData.orc` in the current directory. 
You can then use the following config file to test the `data-validator`. 
It will generate a `report.json` and `report.html`.

```sh
spark-submit \
  --master "local[*]"  \
  data-validator-assembly-0.13.0.jar \
  --config local_validators.yaml \
  --jsonReport report.json  \
  --htmlReport report.html
```

####  `local_validators.yaml`

```yaml
---
numKeyCols: 2
numErrorsToReport: 5
detailedErrors: true

tables:
  - orcFile: testData.orc

    checks:
      - type: rowCount
        minNumRows: 1000

      - type: nullCheck
        column: nullCol
```
