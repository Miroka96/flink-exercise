/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

import scala.util.Try
import scala.util.matching.Regex

object StreamingJob {
  val log_pattern: Regex = "^(\\S+) \\S+ \\S+ \\[(\\d+)\\/(\\w+)\\/(\\d+):(\\d+):(\\d+):(\\d+) (-\\d+)\\] \\\"(\\w+) ([^ \"]+) ?([^\"]+)*\\\" (\\d+) (\\d+|-)$".r
  val filename: String = "NASA_access_log_Aug95"

  def main(args: Array[String]) {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(4)

    val rawData: DataStream[String] = env.readTextFile(filename)

    val parsedData = parseLoglines(rawData)

    /*
    parsedData.map(_ => (1,1))
        .keyBy(0)
        .sum(1)
        .print()*/
    //parsedData.keyBy(_.host)

    env.execute("NASA Homepage Log Analysis")
  }

  def parseLogline(logLine: String): LogLine = {
    val log_pattern(host, day, month, year, hour, minute, second, timezone, httpMethod, ressource, httpVersion, httpReplyCode, replyBytes) = logLine

    LogLine(
      host,
      day.toInt,
      month,
      year.toInt,
      hour.toInt,
      minute.toInt,
      second.toInt,
      timezone,
      httpMethod,
      ressource,
      httpVersion,
      httpReplyCode.toInt,
      Try {
        replyBytes.toInt
      }.toOption
    )
  }

  def parseLoglines(rawData: DataStream[String]): DataStream[LogLine] = {
    rawData.flatMap{ logLine: String =>
      Try{
        parseLogline(logLine)
      }.toOption
    }
  }

  def checkInvalidLoglineParsing(rawData: DataStream[String]): Unit = {
    rawData.filter { logLine =>
      Try {
        parseLogline(logLine)
      }.isFailure
    }.print()
  }
}

case class LogLine(
                    host: String,
                    day: Int,
                    month: String,
                    year: Int,
                    hour: Int,
                    minute: Int,
                    second: Int,
                    timezone: String,
                    httpMethod: String,
                    ressource: String,
                    httpVersion: String,
                    httpReplyCode: Int,
                    replyBytes: Option[Int]
                  )
