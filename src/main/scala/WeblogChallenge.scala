package com.paytm

import java.time.LocalDateTime

import org.apache.spark.{SparkConf, SparkContext}

case class IPPort(ip: String, port: Int)

case class Weblog(
  timestamp: LocalDateTime,
  elb: String,
  clientIPPort: IPPort,
  backendIPPort: Option[IPPort],
  requestProcessingTime: Int,
  backendProcessingTime: Int,
  responseProcessingTime: Int,
  elbStatusCode: String,
  backendStatusCode: String,
  receivedBytes: Int,
  sentBytes: Int,
  request: String,
  userAgent: String,
  sslCipher: String,
  sslProtocol: String
)

object WeblogChallenge {
  val AppName = "PaytmWeblogChallenge"
  val LocalMode = "local"

  def main(args: Array[String]) = {
    val logPath: String = inputFromArgs(args)
    val context = new SparkContext(new SparkConf().setAppName(AppName).setMaster(LocalMode))
    sessionize(context, logPath)
    context.stop()
  }

  def inputFromArgs(args: Array[String]): String = {
    if (args.length < 1) throw new IllegalArgumentException("Path to weblog file is missing")
    args(0)
  }

  def sessionize(context: SparkContext, dataPath: String) {
    println(context.textFile(dataPath)
      .repartition(16).count())
  }
}