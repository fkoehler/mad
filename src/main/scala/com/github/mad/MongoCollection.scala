package com.github.mad

import com.allanbank.mongodb.{MongoCollection => MadMongoCollection, MongoIterator => MadMongoIterator, MongoCursorControl, Durability}
import Implicits._
import resource._
import com.allanbank.mongodb.bson.{Element, Document}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import com.allanbank.mongodb.builder.Find.{Builder => MadBuilder}

class MongoCollection(val underlying: MadMongoCollection) {
  def name = underlying.getName

  def drop() = underlying.drop

  def insert[T](model: T)(implicit c: ToBsonDoc[T]): Int = underlying.insert(c.toBson(model))

  def insert(doc: BsonDoc): Int = underlying.insert(doc)

  def insertAsync(doc: BsonDoc): Future[Int] = underlying.insertAsync(doc).mapTo[Int]

  def insertAsync(doc: BsonDoc, durability: Durability): Future[Int] = underlying.insertAsync(durability, doc).mapTo[Int]

  def save(doc: BsonDoc): Int = underlying.save(doc)

  def save[T](model: T)(implicit c: ToBsonDoc[T]): Int = underlying.save(c.toBson(model))

  def saveAsync(doc: BsonDoc): Future[Int] = underlying.saveAsync(doc).mapTo[Int]

  def update(query: BsonDoc, update: BsonDoc, multi: Boolean = false, upsert: Boolean = false): Long = underlying.update(query, update, multi, upsert)

  def updateAsync(query: BsonDoc, update: BsonDoc, multi: Boolean = false, upsert: Boolean = false, durability: Option[Durability] = None): Future[Long] =
    durability match {
      case Some(d) => underlying.updateAsync(query, update, multi, upsert, d).mapTo[Long]
      case None => underlying.updateAsync(query, update, multi, upsert).mapTo[Long]
    }

  def findOne(doc: BsonDoc): Option[BsonDoc] = Option(underlying.findOne(doc))

  def findOneAs[T](doc: BsonDoc)(implicit c: FromBsonDoc[T]): Option[T] = findOne(doc).map(c.fromBson)

  def findOneAsync(doc: BsonDoc): Future[Option[BsonDoc]] = underlying.findOneAsync(doc).map(Option(_))

  def findOneAsyncAs[T](doc: BsonDoc)(implicit c: FromBsonDoc[T]): Future[Option[T]] = underlying.findOneAsync(doc).map(document => Option(c.fromBson(document)))

  def findOneAsyncAs[T](find: MadBuilder)(implicit c: FromBsonDoc[T]): Future[Option[T]] = underlying.findOneAsync(find).map(doc => Option(c.fromBson(doc)))

  def findOneById(id: Int): Option[BsonDoc] = findOne(Bson.doc("_id" -> id))

  def findOneById(id: String): Option[BsonDoc] = findOne(Bson.doc("_id" -> id))

  def findOneByIdAs[T](id: String)(implicit c: FromBsonDoc[T]): Option[T] = findOneById(id).map(c.fromBson)

  def findOneByIdAsync(id: Int): Future[Option[BsonDoc]] = findOneAsync(Bson.doc("_id" -> id))

  def findOneByIdAsync(id: String): Future[Option[BsonDoc]] = findOneAsync(Bson.doc("_id" -> id))

  def findOneByIdAsyncAs[T](id: String)(implicit c: FromBsonDoc[T]): Future[Option[T]] = findOneByIdAsync(id).map(_.map(c.fromBson))

  def find(find: MadBuilder): ManagedMongoIterator = ManagedMongoIterator(managed(MongoIterator(underlying.find(find))))

  def find(query: BsonDoc): ManagedMongoIterator = ManagedMongoIterator(managed(MongoIterator(underlying.find(query))))

  def findAndApply(query: BsonDoc)(docFunc: (BsonDoc) => Unit): Unit = find(query).acquireAndGet(_.foreach(docFunc))

  def findAndApply(f: MadBuilder)(docFunc: (BsonDoc) => Unit): Unit = find(f).acquireAndGet(_.foreach(docFunc))

  def findAndApplyAs[T](query: BsonDoc)(docFunc: (T) => Unit)(implicit c: FromBsonDoc[T]): Unit =
    find(query).acquireAndGet(_.map(c.fromBson).foreach(docFunc))

  def findAndApplyAs[T](f: MadBuilder)(docFunc: (T) => Unit)(implicit c: FromBsonDoc[T]): Unit =
    find(f).acquireAndGet(_.map(c.fromBson).foreach(docFunc))

  def findAsync(query: BsonDoc): Future[ManagedMongoIterator] = underlying.findAsync(query).map(iter => ManagedMongoIterator(managed(MongoIterator(iter))))

  def findAsync(find: MadBuilder): Future[ManagedMongoIterator] = underlying.findAsync(find.build()).map(iter => ManagedMongoIterator(managed(MongoIterator(iter))))

  def findAsyncAndApply(query: BsonDoc)(docFunc: (BsonDoc) => Unit): Future[Unit] = findAsync(query).map(_.andApply(docFunc))

  def findAsyncAndApply(find: MadBuilder)(docFunc: (BsonDoc) => Unit): Future[Unit] = findAsync(find).map(_.andApply(docFunc))

  def findAsyncAndApplyAs[T](find: MadBuilder)(docFunc: (T) => Unit)(implicit c: FromBsonDoc[T]): Future[Unit] =
    findAsync(find).map(_.acquireAndGet(_.map(c.fromBson).foreach(docFunc)))

  def durability_=(durability: Durability) = underlying.setDurability(durability)

  def durability = underlying.getDurability

  def createIndex(doc: BsonDoc, options: BsonDoc) = underlying.createIndex(options, doc.elements.foldLeft(List[Element]()) {
    (elements, kv) =>
      elements :+ bsonElement2Element(kv._1, kv._2).asElement()
  }: _*)

  def count: Long = underlying.count()

  def count(query: BsonDoc): Long = underlying.count(query)

  def countAsync: Future[Long] = future(underlying.countAsync().get)

  def countAsync(query: BsonDoc): Future[Long] = future(underlying.countAsync(query).get)
}

object Find {
  def apply(): MadBuilder = new MadBuilder()
}

object MongoIterator {
  def apply(underlying: MadMongoIterator[Document]): MongoIterator = new MongoIterator(underlying)
}

object ManagedMongoIterator {
  def apply(underlying: ManagedResource[MongoIterator]): ManagedMongoIterator = new ManagedMongoIterator(underlying)
}

class ManagedMongoIterator(underlying: ManagedResource[MongoIterator]) extends ManagedResource[MongoIterator] {
  def toList(): List[BsonDoc] = underlying.acquireAndGet(_.toList)

  def size: Int = toList().size

  def toListOf[T]()(implicit c: FromBsonDoc[T]): List[T] = toList().map(c.fromBson)

  def andApply(docFunc: (BsonDoc) => Unit): Unit = underlying.acquireAndGet(_.foreach(docFunc))

  def andApplyAs[T](docFunc: (T) => Unit)(implicit c: FromBsonDoc[T]): Unit = underlying.acquireAndGet(_.map(c.fromBson).foreach(docFunc))

  // delegates
  def map[B](f: (MongoIterator) => B): ExtractableManagedResource[B] = underlying.map[B](f)

  def flatMap[B](f: (MongoIterator) => ManagedResource[B]): ManagedResource[B] = underlying.flatMap[B](f)

  def foreach(f: (MongoIterator) => Unit) = underlying.foreach(f)

  def acquireAndGet[B](f: (MongoIterator) => B): B = underlying.acquireAndGet[B](f)

  def acquireFor[B](f: (MongoIterator) => B): Either[List[Throwable], B] = underlying.acquireFor[B](f)

  def toTraversable[B](implicit ev: $less$colon$less[MongoIterator, TraversableOnce[B]]): Traversable[B] = underlying.toTraversable[B]

  def and[B](that: ManagedResource[B]): ManagedResource[(MongoIterator, B)] = underlying.and[B](that)

  def reflect[B]: MongoIterator = underlying.reflect[B]

  @scala.deprecated("Use now instead of !", "5/Apr/2013")
  def ! : MongoIterator = ???

  def now: MongoIterator = underlying.now
}

class MongoIterator(val underlying: MadMongoIterator[Document]) extends Iterator[BsonDoc] with MongoCursorControl {
  // delegates
  def hasNext: Boolean = underlying.hasNext

  def next(): BsonDoc = underlying.next

  def asDocument(): Document = underlying.asDocument

  def close() = underlying.close()

  def batchSize: Int = underlying.getBatchSize

  def batchSize_=(p1: Int) = underlying.setBatchSize(p1)

  def stop() {}

  def getBatchSize: Int = batchSize

  def setBatchSize(p1: Int) = underlying.setBatchSize(p1)

}