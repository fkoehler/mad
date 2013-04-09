package com.github.mad

import concurrent.Await
import Implicits._
import scala.Predef._
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeExample
import org.joda.time.DateTime

class CollectionSpec extends Specification with BeforeExample {

  import Common._

  protected def before = coll.drop

  val simpleDoc = Bson.doc("_id" -> 1, "name" -> "fabian")

  "The collection class" should {
    "insert a simple doc correctly sync" in {
      coll.insert(simpleDoc)
      coll.findOneById(1).get must equalTo(simpleDoc)
    }
    "insert a simple doc correctly async" in {
      Await.ready(coll.insertAsync(simpleDoc), timeout)
      coll.findOneById(1).get must equalTo(simpleDoc)
    }
    "update a simple doc correctly sync" in {
      coll.insert(simpleDoc)
      coll.update(simpleDoc, Bson.doc("$set" -> Bson.doc("name" -> "köhler")))
      coll.findOneById(1).get[String]("name") must equalTo("köhler")
    }
    "multi update a simple doc correctly sync" in {
      coll.insert(simpleDoc)
      coll.insert(Bson.doc("_id" -> 2, "name" -> "fabian"))

      coll.update(Bson.doc("name" -> "fabian"), Bson.doc("$set" -> Bson.doc("name" -> "köhler")), multi = false)
      coll.findOneById(1).get[String]("name") must equalTo("köhler")
      coll.findOneById(2).get[String]("name") must equalTo("fabian")

      coll.update(Bson.doc("name" -> "fabian"), Bson.doc("$set" -> Bson.doc("name" -> "köhler")), multi = true)
      coll.findOneById(2).get[String]("name") must equalTo("köhler")
    }
    "upsert a simple doc correctly sync" in {
      coll.update(Bson.doc("_id" -> 1, "name" -> "köhler"), simpleDoc, upsert = true)
      coll.findOneById(1).get must equalTo(simpleDoc)
    }
    "update a simple doc correctly async" in {
      coll.insert(simpleDoc)
      Await.ready(coll.updateAsync(simpleDoc, Bson.doc("$set" -> Bson.doc("name" -> "köhler"))), timeout)
      coll.findOneById(1).get[String]("name") must equalTo("köhler")
    }
    "save a simple doc correctly sync" in {
      coll.save(simpleDoc)
      coll.findOneById(1).get must equalTo(simpleDoc)
    }
    "save a simple doc correctly async" in {
      Await.ready(coll.saveAsync(simpleDoc), timeout)
      coll.findOneById(1).get must equalTo(simpleDoc)
    }
    "find builder with skip and limit" in {
      coll.save(Bson.doc("_id" -> 1, "name" -> "fabian"))
      coll.save(Bson.doc("_id" -> 2, "name" -> "fabian"))
      coll.save(Bson.doc("_id" -> 3, "name" -> "fabian"))

      val find = Find()
        .query(Bson.doc("name" -> "fabian"))
        .limit(1)
        .skip(1)

      coll.find(find).acquireAndGet(_.size must beEqualTo(1))
      coll.find(find).acquireAndGet(_.next must beEqualTo(Bson.doc("_id" -> 2, "name" -> "fabian")))
    }
    "find a doc by query (managed resource)" in {
      coll.save(simpleDoc)
      coll.find(Bson.doc("_id" -> 1)).acquireAndGet(_.size must beEqualTo(1))
      coll.find(Bson.doc("_id" -> 1)).acquireAndGet(_.next must beEqualTo(simpleDoc))
    }
    "find a doc by query (provide function to execute for every doc)" in {
      coll.save(simpleDoc)
      coll.findAndApply(Bson.doc("_id" -> 1)) {
        doc =>
          doc must beEqualTo(simpleDoc)
      }
    }
    "find a doc by query (managed resource) async" in {
      coll.save(simpleDoc)
      Await.result(coll.findAsync(Bson.doc("_id" -> 1)), timeout).acquireAndGet(_.size must beEqualTo(1))
      Await.result(coll.findAsync(Bson.doc("_id" -> 1)), timeout).acquireAndGet(_.next must beEqualTo(simpleDoc))
    }
    "find a doc by query (provide function to execute for every doc) async" in {
      coll.save(simpleDoc)
      coll.findAsyncAndApply(Bson.doc("_id" -> 1)) {
        doc =>
          doc must beEqualTo(simpleDoc)
      }

      true must beEqualTo(true)
    }
    "create a index in the correct order" in {
      coll.createIndex(Bson.doc("_id" -> 1, "name" -> 1), Bson.doc("background" -> true))
      // TODO how to test this?
    }
    "insert a date time correctly full roundtrip" in {
      val date = DateTime.now
      coll.insert(Bson.doc("_id" -> 99, "datet" -> date))
      coll.findOneById(99).get[DateTime]("datet") must beEqualTo(date)
    }
  }
}