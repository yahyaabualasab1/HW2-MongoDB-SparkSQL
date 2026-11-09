import java.time.LocalDate

case class Student(
  id: Int,
  firstName: String,
  lastName: String,
  dateOfBirth: LocalDate,
  courseIds: List[Int],
  gpa: Double
)