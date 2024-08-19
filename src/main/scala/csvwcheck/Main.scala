/*
 * Copyright 2020 Crown Copyright (Office for National Statistics)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package csvwcheck

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import ch.qos.logback.classic.Level
import com.typesafe.scalalogging.Logger
import csvwcheck.errors.MessageWithCsvContext
import scopt.OParser

import scala.concurrent.Await
import scala.concurrent.duration.Duration

case class Config(
                   inputSchema: Option[String] = None,
                   csvPath: Option[String] = None,
                   logLevel: Level = Level.WARN
                 )


object Main extends App {
  /**
    * Telling scopt how to parse log levels
    */
  implicit val levelScoptRead: scopt.Read[Level] =
    scopt.Read.reads(Level.valueOf)

  private val builder = OParser.builder[Config]
  private val parser = {
    import builder._

    val csvwCheckPackage = this.getClass.getPackage

    OParser.sequence(
      programName(csvwCheckPackage.getImplementationTitle),
      head(csvwCheckPackage.getImplementationTitle, csvwCheckPackage.getImplementationVersion),
      opt[String]('s', "schema")
        .action((schema, c) => c.copy(inputSchema = Some(schema)))
        .text("filename of Schema/metadata file"),
      opt[String]('c', "csv")
        .action((csv, c) => c.copy(csvPath = Some(csv)))
        .text("filename of CSV file"),
      opt[Level]('l', "log-level")
        .action((logLevel, c) => c.copy(logLevel = logLevel))
        .text(s"${Level.OFF}|${Level.ERROR}|${Level.WARN}|${Level.INFO}|${Level.DEBUG}|${Level.TRACE}"),
      help('h', "help").text("prints this usage text")
    )
  }
  private val newLine = sys.props("line.separator")

  OParser.parse(parser, args, Config()) match {
    case Some(config) =>
      val logger = configureLogging(config.logLevel)

      implicit val actorSystem: ActorSystem = ActorSystem("actor-system")


      val numParallelThreads: Int = sys.env.get("PARALLELISM") match {
        case Some(value) => value.toInt
        case None => Runtime.getRuntime.availableProcessors()
      }

      val csvRowBatchSize: Int = sys.env.get("ROW_GROUPING") match {
        case Some(value) => value.toInt
        case None => 1000
      }

      val validator = new Validator(
        config.inputSchema,
        numParallelThreads = numParallelThreads,
        csvRowBatchSize = csvRowBatchSize
      )
      val akkaStream = validator
        .validate()
        .map(warningsAndErrors => {
          if (warningsAndErrors.warnings.nonEmpty) {
            println(Console.YELLOW + "")
            warningsAndErrors.warnings
              .foreach(x => logger.warn(getDescriptionForMessage(x)))
          }
          if (warningsAndErrors.errors.nonEmpty) {
            println(Console.RED + "")
            warningsAndErrors.errors
              .foreach(x => logger.error(getDescriptionForMessage(x)))
            print(Console.RESET + "")
            sys.exit(1)
          }
          println(Console.GREEN + "Valid CSV-W")
          print(Console.RESET + "")
        })
      Await.ready(akkaStream.runWith(Sink.ignore), Duration.Inf)
      actorSystem.terminate()
    case _ =>
      val logger = configureLogging()
      println(Console.RED + "")
      logger.error("Invalid configuration.")
      println(Console.RESET + "")
  }

  private def configureLogging(logLevel: Level = Level.WARN): com.typesafe.scalalogging.Logger  = {
    val rootLogger = Logger("ROOT")
    val underlyingLogger = rootLogger.underlying.asInstanceOf[ch.qos.logback.classic.Logger]
    underlyingLogger.setLevel(logLevel)
    rootLogger
  }

  private def getDescriptionForMessage(
                                        errorMessage: MessageWithCsvContext
                                      ): String = {
    val message = new StringBuilder()
    if (errorMessage.row.nonEmpty) {
      message.append(s"Row: ${errorMessage.row}$newLine")
    }
    if (errorMessage.column.nonEmpty) {
      message.append(s", Column: ${errorMessage.column}$newLine")
    }
    if (errorMessage.content.nonEmpty) {
      message.append(s": ${errorMessage.content}$newLine")
    }

    message.toString()
  }
}
