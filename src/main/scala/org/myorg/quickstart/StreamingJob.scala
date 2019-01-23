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
import java.util
import java.util.Date

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.{TimeCharacteristic, environment}
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger

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
  val log_pattern: Regex = "^(\\S+) - - \\[(\\d\\d)\\/(\\w{1,3})\\/(\\d{4}):(\\d{2}):(\\d{2}):(\\d{2}) (-\\d{4})\\] \\\"(\\w{1,6}) ([^ \"]+) *(HTTP/V?1.0) *\\\" (\\d{3}) (\\d{1,9}|-)$".r
  val filename: String = "NASA_access_log_Aug95"

  def main(args: Array[String]) {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)

    val rawData: DataStream[String] = env.readTextFile(filename)

    //val parsedData = parseLoglines(rawData).assignTimestampsAndWatermarks(new MinuteAssigner)
    val parsedData = parseLoglines(rawData).assignTimestampsAndWatermarks(new MinuteAssigner)

    //countElements(parsedData).print
    //checkInvalidLoglineParsing(rawData).print
    //requestCountPerHost(parsedData).print
    //sumUniqueHostsStream(uniqueHostsStream(parsedData)).print.setParallelism(1)
    hostWithMostRequests(parsedData)

    val uniqeHostCountStream = sumUniqueHostsStream(uniqueHostsStream(parsedData))
    val uniqueHostCountOverOneMonth = uniqeHostCountStream.timeWindowAll(Time.days(31)).max(0)
    uniqueHostCountOverOneMonth.print()

    //println(env.execute("NASA Homepage Log Analysis").getAllAccumulatorResults)
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

  def checkInvalidLoglineParsing(rawData: DataStream[String]): DataStream[String] = {
    rawData.map(parseLogline _).filter(_.host.isEmpty).map(_.raw)
  }

  def requestCountPerHost(parsedData: DataStream[LogLine]): DataStream[(LogLine, Int)] = {
    parsedData.map((_, 1)).keyBy(_._1.host).sum(1)
  }

  def countElements(stream :DataStream[LogLine]): DataStream[Int] = {
    stream.map((_,1)).keyBy(_._2).sum(1).map(_._2)
  }

  def uniqueHostsStream(parsedData: DataStream[LogLine]): DataStream[LogLine] = {
    parsedData.keyBy(x => x.host).filterWithState{
      (log, seenHostState: Option[Set[String]]) => seenHostState match {
        case None => (true, Some(Set(log.host)))
        case Some(seenHosts) => (!seenHosts.contains(log.host), Some(seenHosts + log.host))
      }
    }
  }

  def sumUniqueHostsStream(uniqueHostsStream: DataStream[LogLine]): DataStream[Int] = {
    uniqueHostsStream.map(_ => 1).keyBy(_ => 0).sum(0)
  }

  def hostWithMostRequests(parsedData: DataStream[LogLine]): DataStream[(String, Int)] = {
    parsedData.map(e => (e.host, 1)).keyBy(0).sum(1).keyBy(_ => 0).max(1)
  }
}


