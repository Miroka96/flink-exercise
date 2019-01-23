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

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.Date

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark

import scala.util.Try
import scala.util.matching.Regex

case class LogLine(
                    raw: String = "",
                    host: String = "",
                    day: Int = -1,
                    month: String = "",
                    year: Int = -1,
                    hour: Int = -1,
                    minute: Int = -1,
                    second: Int = -1,
                    timezone: String = "",
                    date: Date = new Date(),
                    httpMethod: String = "",
                    ressource: String = "",
                    httpVersion: String = "",
                    httpReplyCode: Int = -1,
                    replyBytes: Option[Int] = None
                  )

class MinuteAssigner extends AssignerWithPunctuatedWatermarks[LogLine] {

  override def extractTimestamp(element: LogLine, previousElementTimestamp: Long): Long = {
    element.date.getTime
  }

  override def checkAndGetNextWatermark(lastElement: LogLine, extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp / 60)
  }
}


object StreamingJob {
  val log_pattern: Regex = "^(\\S+) \\S+ \\S+ \\[(\\d+)\\/(\\w+)\\/(\\d+):(\\d+):(\\d+):(\\d+) (-\\d+)\\] \\\"(\\w+) ([^ \"]+) ?([^\"]+)*\\\" (\\d+) (\\d+|-)$".r
  val filename: String = "NASA_access_log_Aug95"

  def main(args: Array[String]) {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)

    val rawData: DataStream[String] = env.readTextFile(filename)

    val parsedData = parseLoglines(rawData).assignTimestampsAndWatermarks(new MinuteAssigner)

    //requestCountPerHost(parsedData)
    //checkInvalidLoglineParsing(rawData) - None
    //requestCountPerHost(parsedData)


    val uniqueHosts = parsedData.keyBy(x => x.host).filterWithState{
      (log, seenHostState: Option[Set[String]]) => seenHostState match {
        case None => (true, Some(Set(log.host)))
        case Some(seenHosts) => (!seenHosts.contains(log.host), Some(seenHosts + log.host))
      }
    }

    val numberUniqueWords = uniqueHosts.keyBy(x => 0).mapWithState{
      (host, counterState: Option[Int]) =>
        counterState match {
          case None => (1, Some(1))
          case Some(counter) => (counter + 1, Some(counter + 1))
        }
    }.setParallelism(1)

    numberUniqueWords.print.setParallelism(1)

    env.execute("NASA Homepage Log Analysis")
  }

  def parseLogline(logLine: String): LogLine = {
    logLine match {
      case log_pattern(host, day, month, year, hour, minute, second, timezone, httpMethod, ressource, httpVersion, httpReplyCode, replyBytes) =>
        LogLine(
          logLine,
          host,
          day.toInt,
          month,
          year.toInt,
          hour.toInt,
          minute.toInt,
          second.toInt,
          timezone,
          new Date(LocalDateTime.parse(day+"/"+month+"/"+year+":"+hour+":"+minute+":"+second,
            DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss")).toEpochSecond(ZoneOffset.of(timezone))),
          httpMethod,
          ressource,
          httpVersion,
          httpReplyCode.toInt,
          Try {
            replyBytes.toInt
          }.toOption
        )
      case _ => LogLine(raw = logLine)
    }

  }


  def parseLoglines(rawData: DataStream[String]): DataStream[LogLine] = {
    rawData.map(parseLogline _).filter(_.host.nonEmpty)
  }

  def checkInvalidLoglineParsing(rawData: DataStream[String]): Unit = {
    rawData.map(parseLogline _).filter(_.host.isEmpty).print()
  }

  def requestCountPerHost(parsedData: DataStream[LogLine]): Unit = {
    val counts = parsedData
      .keyBy(_.host)
      .mapWithState((log :LogLine, count: Option[Int]) =>
      count match {
        case Some(c) => ( (log.host, c), Some(c + 1) )
        case None => ( (log.host, 1), Some(1) )
      })

    counts.print
      .setParallelism(1)

  }


}


