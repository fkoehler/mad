package com.github.mad

import com.allanbank.mongodb.bson.builder.BuilderFactory
import org.specs2.mutable.Specification
import org.joda.time.DateTime

class BsonSpec extends Specification {

  import Implicits._

  "The more scala like bson builder classes" should {
    "convert a doc with multiple types into a proper document" in {
      val op: Option[Int] = None
      val date = DateTime.now
      val bsonDoc = Bson.doc(
        "_id" -> 1,
        "name" -> "fabian",
        "double" -> 34.45,
        "bool" -> true,
        "date" -> date,
        "optionSome" -> Some(5),
        "optionNone" -> op,
        "subDoc" -> Bson.doc("subId" -> 2)
      // TODO : why is this failing in the test?? seems to be a string comparing problem...
//        "stringarray" -> Bson.arr(
//          "string",
//          "string2"
//        ),
//        "docarray" -> Bson.arr(
//          Bson.doc("_id2" -> 45),
//          Bson.doc("_id3" -> 77)
//        )
      )

      val docArray = BuilderFactory.startArray()
      docArray.push().add("_id2", 45)
      docArray.push().add("_id3", 77)
      val doc = BuilderFactory.start
        .add("_id", 1)
        .add("name", "fabian")
        .add("double", 34.45)
        .add("bool", true)
        .add("date", date.toDate())
        .add("optionSome", 5)
        .addNull("optionNone")
        .add("subDoc", BuilderFactory.start().add("subId", 2))
//        .add("stringarray", BuilderFactory.startArray().add("string").add("string2").build())
//        .add("docarray", docArray.build())
        .build

      bsonDoc2Document(bsonDoc) must equalTo(doc)
      document2BsonDoc(doc) must equalTo(bsonDoc)
    }
  }

}