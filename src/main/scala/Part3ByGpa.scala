import scala.io.StdIn
import scala.util.Try
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Sorts

object Part3ByGpa {
  def main(args: Array[String]): Unit = {
    print("Enter minimum GPA (0.0 to 4.0): ")
    val input = StdIn.readLine()

    val minimumGpa = Option(input)
      .flatMap(value => Try(value.trim.toDouble).toOption)
      .filter(value => value >= 0.0 && value <= 4.0)

    minimumGpa match {
      case Some(gpa) =>
        print("Enter course ID (press Enter to skip): ")
        val courseInput = Option(StdIn.readLine())
          .getOrElse("")
          .trim

        val courseId =
          if (courseInput.isEmpty) None
          else Try(courseInput.toInt).toOption

        if (courseInput.nonEmpty && courseId.isEmpty) {
          println("Invalid input: course ID must be an integer.")
          return
        }

        val filter = courseId match {
          case Some(id) =>
            Filters.and(
              Filters.gt("gpa", gpa),
              Filters.eq("courseIds", id)
            )

          case None =>
            Filters.gt("gpa", gpa)
        }

        val client = MongoClients.create(
          "mongodb://localhost:27017/?serverSelectionTimeoutMS=5000"
        )

        try {
          val collection = client
            .getDatabase("university")
            .getCollection("students")

          val query = collection
            .find(filter)
            .sort(Sorts.descending("gpa"))

          println(s"Students with GPA strictly greater than $gpa:")
          courseId.foreach(id => println(s"Course filter: $id"))

          val cursor = query.iterator()
          var count = 0

          try {
            while (cursor.hasNext) {
              val student = cursor.next()

              val studentId = student.getInteger("_id")
              val firstName = student.getString("firstName")
              val lastName = student.getString("lastName")
              val studentGpa = student.getDouble("gpa")

              println(
                s"$studentId | $firstName $lastName | GPA: $studentGpa"
              )

              count += 1
            }
          } finally {
            cursor.close()
          }

          println(s"Number of matches: $count")
        } finally {
          client.close()
        }

      case None =>
        println("Invalid input: GPA must be a number between 0.0 and 4.0.")
    }
  }
}