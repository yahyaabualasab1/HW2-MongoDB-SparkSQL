import scala.io.StdIn
import scala.util.Try
import scala.jdk.CollectionConverters._
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Sorts
import org.bson.Document

object Part2ByCourse {
  def usesCourseIndex(value: Any): Boolean = value match {
    case document: Document =>
      val isCourseScan =
        document.get("stage") == "IXSCAN" &&
        document.get("indexName") == "courseIds_1"

      isCourseScan ||
        document.values().asScala.exists(value => usesCourseIndex(value))

    case list: java.util.List[_] =>
      list.asScala.exists(value => usesCourseIndex(value))

    case _ =>
      false
  }

  def main(args: Array[String]): Unit = {
    print("Enter course ID: ")
    val input = StdIn.readLine()

    val courseId = Option(input)
      .flatMap(value => Try(value.trim.toInt).toOption)

    courseId match {
      case Some(id) =>
        val client = MongoClients.create(
          "mongodb://localhost:27017/?serverSelectionTimeoutMS=5000"
        )

        try {
          val collection = client
            .getDatabase("university")
            .getCollection("students")

          val query = collection
            .find(Filters.eq("courseIds", id))
            .sort(Sorts.ascending("lastName"))

          val cursor = query.iterator()
          var count = 0

          println(s"Students in course $id:")

          try {
            while (cursor.hasNext) {
              val student = cursor.next()

              val studentId = student.getInteger("_id")
              val firstName = student.getString("firstName")
              val lastName = student.getString("lastName")
              val gpa = student.getDouble("gpa")

              println(s"$studentId | $firstName $lastName | GPA: $gpa")
              count += 1
            }
          } finally {
            cursor.close()
          }

          println(s"Number of matches: $count")

          val explanation = query.explain()

          val winningPlan = explanation
            .get("queryPlanner", classOf[Document])
            .get("winningPlan", classOf[Document])

          println("Winning plan:")
          println(winningPlan.toJson())

          println(
            s"courseIds index used: ${usesCourseIndex(winningPlan)}"
          )
        } finally {
          client.close()
        }

      case None =>
        println("Invalid input: course ID must be an integer.")
    }
  }
}