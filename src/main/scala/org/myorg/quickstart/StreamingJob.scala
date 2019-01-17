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

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.util.Try

object StreamingJob {
  def main(args: Array[String]) {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val rawData: DataStream[String] = env.readTextFile("NASA_access_log_Aug95")

    val log_pattern = "^(\\S+) \\S+ \\S+ \\[(\\d+)\\/(\\w+)\\/(\\d+):(\\d+):(\\d+):(\\d+) (-\\d+)\\] \\\"(\\w+) ([^ \"]+) ?([^\"]+)*\\\" (\\d+) (\\d+|-)$".r

    val parsedData = rawData.flatMap{ logLine =>
      val log_pattern(host, day, month, year, hour, minute, second, timezone, httpMethod, ressource, httpVersion, httpReplyCode, replyBytes) = logLine
      Try {
        LogLine(
          host,
          day.toInt,
          month,
          year.toInt,
          hour.toInt,
          minute.toInt,
          second.toInt,
          timezone,
          LocalDateTime.parse(day+"/"+month+"/"+year+":"+hour+":"+minute+":"+second,
            DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss")),
          httpMethod,
          ressource,
          httpVersion,
          httpReplyCode.toInt,
          Try{replyBytes.toInt}.toOption
        )
      }.toOption
    }

    //print number of items
    /*parsedData.keyBy(_.host).mapWithState((log, count: Option[Int]) =>
      count match {
        case Some(c) => ( log, Some(c + 1) )
        case None => ( log, Some(1) )
      }).print.setParallelism(1)*/

    // sort by timestamp

    parsedData.keyBy(_.host).map(log => log).print()



    //parsedData.keyBy(_.host)

    env.execute("NASA Homepage Log Analysis")
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
                    date: LocalDateTime,
                    httpMethod: String,
                    ressource: String,
                    httpVersion: String,
                    httpReplyCode: Int,
                    replyBytes: Option[Int]
                  )
