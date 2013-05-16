package com.github.mad

import com.allanbank.mongodb.bson.builder.BuilderFactory
import org.specs2.mutable.Specification
import org.joda.time.DateTime

class DocumentImplicitsSpec extends Specification {

  import Implicits._

  "The more scala like builder classes" should {
    "work" in {
      val date = DateTime.now
      val bsonDoc = Bson.doc(
        "_id" -> 12345,
        "name" -> "fabian",
        "double" -> 34.456,
        "date" -> date,
        "nullValue" -> null
      )

      val doc = BuilderFactory.start
        .add("_id", 12345)
        .add("name", "fabian")
        .add("double", 34.456)
        .add("date", date.toDate)
        .addNull("nullValue")
        .build

      bsonDoc2Document(bsonDoc) must equalTo(doc)
    }
  }

}