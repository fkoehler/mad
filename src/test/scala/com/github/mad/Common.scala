package com.github.mad

import com.allanbank.mongodb.MongoClientConfiguration
import scala.concurrent.duration._
import scala.language.postfixOps

object Common {

  val config = new MongoClientConfiguration
  config.addServer("localhost:27017")

  val client = MongoClient(config)
  val db = client("madtest")
  val coll = db("test")

  val timeout = 3 seconds

}
