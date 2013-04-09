package com.github.mad

import com.allanbank.mongodb.{MongoClient => MadMongoClient, MongoDatabase => MadMongoDatabase, Durability, MongoFactory, MongoClientConfiguration}
import Implicits._
import com.allanbank.mongodb.gridfs.GridFs

object MongoClient {
  val defaultDurability = Durability.ACK

  def apply(config: MongoClientConfiguration): MongoClient = {
    new MongoClient(MongoFactory.createClient(config))
  }

  def apply(config: String): MongoClient = {
    new MongoClient(MongoFactory.createClient(config))
  }
}

class MongoClient(val underlying: MadMongoClient) {
  def apply(databaseName: String): MongoDatabase = new MongoDatabase(underlying.getDatabase(databaseName))
}

class MongoDatabase(val underlying: MadMongoDatabase) {

  def name = underlying.getName

  def apply(collectionName: String): MongoCollection = new MongoCollection(underlying.getCollection(collectionName))

  def durability_=(durability: Durability) = underlying.setDurability(durability)

  def durability = underlying.getDurability

  def runCommand(command: String, options: BsonDoc) = underlying.runCommand(command, options)

  def runCommand(command: String, value: String, options: BsonDoc = Bson.doc()) = underlying.runCommand(command, value, options)

  def runAdminCommand(command: String, options: BsonDoc) = underlying.runAdminCommand(command, options)

  def runAdminCommand(command: String, value: String, options: BsonDoc = Bson.doc()) = underlying.runAdminCommand(command, value, options)

  def gridFs: GridFs = new GridFs(underlying)

  def gridFs(rootName: String): GridFs = new GridFs(underlying, rootName)

}