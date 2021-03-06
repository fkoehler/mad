package com.github.mad

import com.allanbank.mongodb.bson.builder.BuilderFactory
import com.allanbank.mongodb.bson.{ElementAssignable, Element, ElementType, Document}
import com.allanbank.mongodb.bson.element._
import java.util.Date
import org.joda.time.DateTime

object Implicits extends BsonDocImplicits with BsonImplicits

trait BsonDocImplicits {

  import scala.language.implicitConversions
  import scala.collection.JavaConversions._

  implicit def bsonDoc2Document(bsonDoc: BsonDoc): Document = {
    val builder = BuilderFactory.start

    for ((key, element) <- bsonDoc.elements) {
      element match {
        case BsonArray(v) => {
          val ab = BuilderFactory.startArray()
          v.foreach(elem => ab.add(bsonElement2Element(key, elem)))
          builder.add(key, ab.build())
        }
        case _ => builder.add(bsonElement2Element(key, element))
      }
    }

    builder.build
  }

  implicit def bsonElement2Element(key: String, element: BsonElement): ElementAssignable = {
    element match {
      case BsonObjectId(v) => new ObjectIdElement(key, new ObjectId(v))
      case BsonDouble(v) => new DoubleElement(key, v)
      case BsonString(v) =>
        Option(v) match {
          case Some(v) => new StringElement(key, v)
          case None => new NullElement(key)
        }
      case BsonBoolean(v) => new BooleanElement(key, v)
      case BsonInt(v) => new IntegerElement(key, v)
      case BsonLong(v) => new LongElement(key, v)
      case BsonDateTime(v) => new TimestampElement(key, v.getMillis)
      case doc@BsonDoc(v) => new DocumentElement(key, bsonDoc2Document(doc))
      case null => new NullElement(key)
      case BsonNull => new NullElement(key)
      case BsonAny(v) => v match {
        case v: String => new StringElement(key, v)
        case v: Int => new IntegerElement(key, v)
        case v: Double => new DoubleElement(key, v)
        case v: Long => new LongElement(key, v)
        case v: DateTime => new TimestampElement(key, v.getMillis)
        case v: Boolean => new BooleanElement(key, v)
      }
      case v @ _ => throw new RuntimeException("not yet implemented for value: " + v)
    }
  }

  implicit def document2BsonDoc(doc: Document): BsonDoc = {
    if (doc == null) // TODO evtl. draussen abfangen??
      return null

    var bsonDoc = Bson.doc()

    for (element <- doc.getElements()) {
      bsonDoc += element.getName -> elementToBsonElement(element)
    }

    bsonDoc
  }

  private def elementToBsonElement(element: Element): BsonElement = {
    element.getType match {
      case ElementType.OBJECT_ID => BsonObjectId(elemValAs[ObjectId](element).toString)
      case ElementType.INTEGER => BsonInt(elemValAs[Int](element))
      case ElementType.DOUBLE => BsonDouble(elemValAs[Double](element))
      case ElementType.LONG => BsonLong(elemValAs[Long](element))
      case ElementType.STRING => BsonString(element.getValueAsString)
      case ElementType.DOCUMENT => document2BsonDoc(elemValAs[Document](element))
      case ElementType.NULL => BsonNull
      case ElementType.BOOLEAN => BsonBoolean(elemValAs[Boolean](element))
      case ElementType.UTC_TIMESTAMP => BsonDateTime(new DateTime(elemValAs[Date](element)))
      case ElementType.ARRAY => element.getValueAsObject.asInstanceOf[Array[Element]].foldLeft(Bson.arr()) {
        (array, element) =>
          array :+ elementToBsonElement(element)
      }
      case _ => throw new RuntimeException("not yet implemented. go ahead: " + element.getName + " " + element.getType + " " + element)
    }
  }

  private def elemValAs[T](element: Element): T = element.getValueAsObject.asInstanceOf[T]

}
