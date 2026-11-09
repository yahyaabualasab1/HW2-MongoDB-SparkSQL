import scala.io.Source
import java.time.{LocalDate, ZoneOffset}
import java.util.Date
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Indexes
import org.bson.Document

object Part1Import {
  def parseStudent(line: String): Student = {
    val fields = line.split(",(?![^\\[]*\\])")

    require(fields.length == 6, s"Invalid student row: $line")

    val courseText = fields(4).trim
      .stripPrefix("[")
      .stripSuffix("]")

    val courseIds =
      if (courseText.isEmpty) List.empty[Int]
      else courseText.split(",").map(_.trim.toInt).toList

    Student(
      id = fields(0).trim.toInt,
      firstName = fields(1).trim,
      lastName = fields(2).trim,
      dateOfBirth = LocalDate.parse(fields(3).trim),
      courseIds = courseIds,
      gpa = fields(5).trim.toDouble
    )
  }

  def toDocument(student: Student): Document = {
    val birthDate = Date.from(
      student.dateOfBirth
        .atStartOfDay(ZoneOffset.UTC)
        .toInstant
    )

    new Document("_id", Int.box(student.id))
      .append("firstName", student.firstName)
      .append("lastName", student.lastName)
      .append("dateOfBirth", birthDate)
      .append(
        "courseIds",
        student.courseIds.map(id => Int.box(id)).asJava
      )
      .append("gpa", Double.box(student.gpa))
  }

  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("data/hw2-data.csv", "UTF-8")

    try {
      val students = source.getLines()
        .drop(1)
        .filter(_.trim.nonEmpty)
        .map(parseStudent)
        .toList

      println(s"Number of students: ${students.size}")
      students.take(3).foreach(println)

      val documents = students.map(toDocument)
      println("First MongoDB document: " + documents.head.toJson())

      // حفظ مستند JSON واحد في كل سطر
      val outputPath = Paths.get("data/students.json")
      val writer = Files.newBufferedWriter(
        outputPath,
        StandardCharsets.UTF_8
      )

      try {
        documents.foreach { document =>
          writer.write(document.toJson())
          writer.newLine()
        }
      } finally {
        writer.close()
      }

      println(s"Saved ${documents.size} documents to $outputPath")

      val client = MongoClients.create(
        "mongodb://localhost:27017/?serverSelectionTimeoutMS=5000"
      )

      try {
        val database = client.getDatabase("university")
        val collection = database.getCollection("students")

        // إعادة إنشاء مجموعة طلاب الواجب عند كل تشغيل
        collection.drop()

        // إدخال كل المستندات باستدعاء واحد
        val result = collection.insertMany(documents.asJava)

        println(s"Inserted documents: ${result.getInsertedIds.size()}")
        println(s"Documents in collection: ${collection.countDocuments()}")

        // إنشاء الفهارس بعد الإدخال
        collection.createIndex(Indexes.ascending("courseIds"))
        collection.createIndex(Indexes.ascending("gpa"))

        println("Indexes:")

        val cursor = collection.listIndexes().iterator()

        try {
          while (cursor.hasNext) {
            println(cursor.next().toJson())
          }
        } finally {
          cursor.close()
        }
      } finally {
        client.close()
      }
    } finally {
      source.close()
    }
  }
}