package com.github.mad

import com.allanbank.mongodb.{MongoCollection => MadMongoCollection, MongoIterator => MadMongoIterator, Callback, MongoCursorControl, Durability}
import Implicits._
import resource._
import com.allanbank.mongodb.bson.{Element, Document}
import scala.concurrent.ExecutionContext.Implicits.global
import com.allanbank.mongodb.builder.Find.{Builder => MadBuilder}
import scala.concurrent.{Promise, Future}

class MongoCollection(val underlying: MadMongoCollection) {
  def name = underlying.getName

  def drop() = underlying.drop()

  def delete() = underlying.delete(Bson.doc())

  def insert[T](model: T)(implicit c: ToBsonDoc[T]): Int = underlying.insert(c.toBson(model))

  def insert(doc: BsonDoc): Int = underlying.insert(doc)

  def insertAsync(doc: BsonDoc): Future[Int] =
    wrapCallback[Integer]((cb: Callback[Integer]) => underlying.insertAsync(cb, doc)).mapTo[Int]

  def insertAsync(doc: BsonDoc, durability: Durability): Future[Int] =
    wrapCallback[Integer]((cb: Callback[Integer]) => underlying.insertAsync(cb, durability, doc)).mapTo[Int]

  def save(doc: BsonDoc): Int = underlying.save(doc)

  def save[T](model: T)(implicit c: ToBsonDoc[T]): Int = underlying.save(c.toBson(model))

  def saveAsync(doc: BsonDoc): Future[Int] =
    wrapCallback[Integer]((cb: Callback[Integer]) => underlying.saveAsync(cb, doc)).mapTo[Int]

  def update(query: BsonDoc, update: BsonDoc, multi: Boolean = false, upsert: Boolean = false): Long = underlying.update(query, update, multi, upsert)

  def updateAsync(query: BsonDoc, update: BsonDoc, multi: Boolean = false, upsert: Boolean = false, durability: Option[Durability] = None): Future[Long] =
    durability match {
      case Some(d) => wrapCallback[java.lang.Long]((cb: Callback[java.lang.Long]) => underlying.updateAsync(cb, query, update, multi, upsert, d)).mapTo[Long]
      case None => wrapCallback[java.lang.Long]((cb: Callback[java.lang.Long]) => underlying.updateAsync(cb, query, update, multi, upsert)).mapTo[Long]
    }

  def findOne(doc: BsonDoc): Option[BsonDoc] = Option(underlying.findOne(doc))

  def findOneAs[T](doc: BsonDoc)(implicit c: FromBsonDoc[T]): Option[T] = findOne(doc).map(c.fromBson)

  def findOneAsync(doc: BsonDoc): Future[Option[BsonDoc]] =
    wrapCallback((cb: Callback[Document]) => underlying.findOneAsync(cb, doc)).map(Option(_))

  def findOneAsyncAs[T](doc: BsonDoc)(implicit c: FromBsonDoc[T]): Future[Option[T]] =
    wrapCallback((cb: Callback[Document]) => underlying.findOneAsync(cb, doc)).map(document => Option(document) match {
    case Some(doc) => Some(c.fromBson(doc))
    case None => None
  })

  def findOneAsyncAs[T](find: MadBuilder)(implicit c: FromBsonDoc[T]): Future[Option[T]] =
    wrapCallback((cb: Callback[Document]) => underlying.findOneAsync(cb, find)).map(doc => Option(doc) match {
      case Some(doc) => Some(c.fromBson(doc))
      case None => None
    })

  def findOneById(id: Int): Option[BsonDoc] = findOne(Bson.doc("_id" -> id))

  def findOneById(id: String): Option[BsonDoc] = findOne(Bson.doc("_id" -> id))

  def findOneByIdAs[T](id: String)(implicit c: FromBsonDoc[T]): Option[T] = findOneById(id).map(c.fromBson)

  def findOneByIdAsync(id: Int): Future[Option[BsonDoc]] = findOneAsync(Bson.doc("_id" -> id))

  def findOneByIdAsync(id: String): Future[Option[BsonDoc]] = findOneAsync(Bson.doc("_id" -> id))

  def findOneByIdAsyncAs[T](id: String)(implicit c: FromBsonDoc[T]): Future[Option[T]] = findOneByIdAsync(id).map(_.map(c.fromBson))

  def managedFind(find: MadBuilder): ManagedMongoIterator = ManagedMongoIterator(managed(MongoIterator(underlying.find(find))))

  def managedFind(query: BsonDoc): ManagedMongoIterator = ManagedMongoIterator(managed(MongoIterator(underlying.find(query))))

  def managedFindAndApply(query: BsonDoc)(docFunc: (BsonDoc) => Unit): Unit = managedFind(query).acquireAndGet(_.foreach(docFunc))

  def managedFindAndApply(f: MadBuilder)(docFunc: (BsonDoc) => Unit): Unit = managedFind(f).acquireAndGet(_.foreach(docFunc))

  def managedFindAndApplyAs[T](query: BsonDoc)(docFunc: (T) => Unit)(implicit c: FromBsonDoc[T]): Unit =
    managedFind(query).acquireAndGet(_.map(c.fromBson).foreach(docFunc))

  def managedFindAndApplyAs[T](f: MadBuilder)(docFunc: (T) => Unit)(implicit c: FromBsonDoc[T]): Unit =
    managedFind(f).acquireAndGet(_.map(c.fromBson).foreach(docFunc))

  def managedFindAsync(query: BsonDoc): Future[ManagedMongoIterator] =
    wrapCallback((cb: Callback[MadMongoIterator[Document]]) => underlying.findAsync(cb, query)).map(iter => ManagedMongoIterator(managed(MongoIterator(iter))))

  def managedFindAsync(find: MadBuilder): Future[ManagedMongoIterator] =
    wrapCallback((cb: Callback[MadMongoIterator[Document]]) => underlying.findAsync(cb, find.build())).map(iter => ManagedMongoIterator(managed(MongoIterator(iter))))

  def managedFindAsyncAndApply(query: BsonDoc)(docFunc: (BsonDoc) => Unit): Future[Unit] = managedFindAsync(query).map(_.andApply(docFunc))

  def managedFindAsyncAndApply(find: MadBuilder)(docFunc: (BsonDoc) => Unit): Future[Unit] = managedFindAsync(find).map(_.andApply(docFunc))

  def managedFindAsyncAndApplyAs[T](find: MadBuilder)(docFunc: (T) => Unit)(implicit c: FromBsonDoc[T]): Future[Unit] =
    managedFindAsync(find).map(_.acquireAndGet(_.map(c.fromBson).foreach(docFunc)))

  def find(query: BsonDoc): MongoIterator = MongoIterator(underlying.find(query))

  def find(find: MadBuilder): MongoIterator = MongoIterator(underlying.find(find))

  def findAs[T](find: MadBuilder)(implicit c: FromBsonDoc[T]): Iterator[T] = {
    val underlyingIter = underlying.find(find)
    new Iterator[T] {
      def hasNext: Boolean = underlyingIter.hasNext
      def next(): T = c.fromBson(underlyingIter.next())
    }
  }

  def findAndApply(query: BsonDoc)(docFunc: (BsonDoc) => Unit): Unit = find(query).foreach(docFunc)

  def findAndApply(f: MadBuilder)(docFunc: (BsonDoc) => Unit): Unit = find(f).foreach(docFunc)

  def findAndApplyAs[T](query: BsonDoc)(docFunc: (T) => Unit)(implicit c: FromBsonDoc[T]): Unit =
    find(query).map(c.fromBson).foreach(docFunc)

  def findAndApplyAs[T](f: MadBuilder)(docFunc: (T) => Unit)(implicit c: FromBsonDoc[T]): Unit =
    find(f).map(c.fromBson).foreach(docFunc)

  def findAsync(query: BsonDoc): Future[MongoIterator] =
    wrapCallback((cb: Callback[MadMongoIterator[Document]]) => underlying.findAsync(cb, query)).map(MongoIterator(_))

  def findAsync(find: MadBuilder): Future[MongoIterator] =
    wrapCallback((cb: Callback[MadMongoIterator[Document]]) => underlying.findAsync(cb, find.build())).map(MongoIterator(_))

  def findAsyncAndApply(query: BsonDoc)(docFunc: (BsonDoc) => Unit): Future[Unit] = findAsync(query).map(_.foreach(docFunc))

  def findAsyncAndApply(find: MadBuilder)(docFunc: (BsonDoc) => Unit): Future[Unit] = findAsync(find).map(_.foreach(docFunc))

  def findAsyncAndApplyAs[T](find: MadBuilder)(docFunc: (T) => Unit)(implicit c: FromBsonDoc[T]): Future[Unit] =
    findAsync(find).map(_.map(c.fromBson).foreach(docFunc))

  def durability_=(durability: Durability) = underlying.setDurability(durability)

  def durability = underlying.getDurability

  def createIndex(doc: BsonDoc, options: BsonDoc) = underlying.createIndex(options, doc.elements.foldLeft(List[Element]()) {
    (elements, kv) =>
      elements :+ bsonElement2Element(kv._1, kv._2).asElement()
  }: _*)

  def count: Long = underlying.count()

  def count(query: BsonDoc): Long = underlying.count(query)

  def countAsync: Future[Long] = wrapCallback((cb: Callback[java.lang.Long]) => underlying.countAsync(cb)).mapTo[Long]

  def countAsync(query: BsonDoc): Future[Long] = wrapCallback((cb: Callback[java.lang.Long]) => underlying.countAsync(cb, query)).mapTo[Long]

  private def wrapCallback[T](opExecutor: (Callback[T]) => Unit): Future[T] = {
    val result = Promise[T]
    val callback = new Callback[T] {
      def callback(p1: T) = result.success(p1)

      def exception(p1: Throwable) = result.failure(p1)
    }
    opExecutor.apply(callback)
    result.future
  }

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