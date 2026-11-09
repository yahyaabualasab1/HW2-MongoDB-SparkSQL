# HW2 — MongoDB and Spark SQL

A student-records pipeline implemented in Scala:
CSV → MongoDB → Spark SQL → MongoDB course summaries.

## Requirements

Versions used:
- Java 23.0.1
- Scala 2.13.18
- sbt 1.10.7
- Apache Spark 4.2.0
- MongoDB running locally on port 27017
- MongoDB Spark Connector 11.1.0

Java, sbt and Spark commands must be available on PATH.

## Project Structure

- `Student.scala`: shared student model.
- `Part1Import.scala`: parse CSV, export JSON, insert students and create indexes.
- `Part2ByCourse.scala`: query students by course and inspect index usage.
- `Part3ByGpa.scala`: query by GPA with an optional course filter.
- `Part4SparkSQL.scala`: run SQL analyses and save course summaries.

## Running on Windows

Run all commands from the project root.

### 1. Import and Index

```powershell
sbt.bat "runMain Part1Import"
```

Reads `data/hw2-data.csv`, writes `data/students.json`, and inserts
100 student documents into `university.students`.

**Each run drops and recreates the students collection.**

### 2. Query by Course

```powershell
sbt.bat "runMain Part2ByCourse"
```

Enter an integer course ID. Results are sorted by last name.
The program prints the winning query plan and checks whether
the `courseIds_1` index was used.

### 3. Query by GPA

```powershell
sbt.bat "runMain Part3ByGpa"
```

Enter a GPA between 0.0 and 4.0. Only students with a strictly
greater GPA are returned, sorted by GPA descending.

Enter an optional course ID, or press Enter to skip it.

### 4. Spark SQL Analysis

Build the project:

```powershell
sbt.bat package
```

Run with Spark and the MongoDB connector:

```powershell
spark-submit.cmd --class Part4SparkSQL --packages org.mongodb.spark:mongo-spark-connector_2.13:11.1.0 target/scala-2.13/hw2-mongodb-sparksql_2.13-0.1.0.jar
```

Uses Option A: reads directly from `university.students`.

Analyses include:
- Course and GPA filtering.
- Exploding course arrays into enrollments.
- Per-course statistics and course-count distribution.
- Statistics by birth year.
- Top three GPA ranks per course.
- Student pairs with matching full names.
- Students above the average of every enrolled course.

`DENSE_RANK` retains all ties within the top three GPA ranks,
so a course may return more than three students.

The per-course summary is written to `university.course_stats`
and read back for verification.

**Each run overwrites the course_stats collection.**

## Verified Results

| Check | Result |
|---|---:|
| Imported students | 100 |
| Students in course 103 | 36 |
| Students with GPA > 3.5 | 49 |
| Course 103 and GPA > 3.5 | 9 |
| Total enrollments | 218 |
| Students with one / two / three courses | 8 / 66 / 26 |
| Pairs sharing the same full name | 7 |
| Students above every enrolled course average | 50 |
| Saved course summaries | 5 |

## MongoDB and Spark

MongoDB supports indexed document queries. The tested course
query used an IXSCAN on the multikey courseIds index.

Spark SQL expresses array expansion, aggregations, joins and
window functions over DataFrames. The course and GPA query
results matched their MongoDB equivalents.