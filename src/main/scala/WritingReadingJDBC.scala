import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import Utils._

object WritingReadingJDBC {

  /**
   * Runs a sequence of JDBC read and write operations on the provided DataFrame.
   *
   * This function demonstrates various ways to interact with JDBC sources, including:
   * - Writing to JDBC with different batch sizes and repartitioning strategies.
   * - Reading from JDBC using partitioning by date and day_of_month for scalability.
   * - Measuring and logging execution times for each operation.
   *
   * @param df The input DataFrame to be used in JDBC operations.
   */
  def run(df: DataFrame): Unit = {

    val spark = df.sparkSession

    // Default JDBC write with batch size of 1000
    printTime(
      "JDBC write using batchSize=1000 (default)",
      writeDfToJdbc(df, "dates")
    )

    // JDBC write with a larger batch size of 10000 for performance optimization
    printTime(
      "JDBC write using batchSize=10000",
      writeDfToJdbc(df, "dates", Map("batchSize" -> "10000"))
    )

    // JDBC write after repartitioning the DataFrame by the 'day_of_month' column
    printTime(
      "JDBC write repartitioning by day_of_month",
      Utils.writeDfToJdbc(df.repartition(col("day_of_month")), "dates")
    )

    // Get the start and end dates for the current year (assumed utility function)
    val (startDate, endDate) = getStartEndDates

    // Options for JDBC read partitioning by the 'date' column
    val jdbcReadByDateOptions = Map(
      "partitionColumn" -> "date",
      "lowerBound" -> startDate.toString,
      "upperBound" -> endDate.toString,
      "numPartitions" -> spark.sparkContext.defaultParallelism.toString
    )

    // Read from JDBC using date-based partitioning and log the number of partitions
    printTime(
      "",
      printNumParts(
        "JDBC read partitioning by date",
        readDfFromJdbc(spark, "dates", jdbcReadByDateOptions)
      )
    )

    // Options for JDBC read partitioning by the 'day_of_month' column
    val jdbcReadByDayOfMonthOptions = Map(
      "partitionColumn" -> "day_of_month",
      "lowerBound" -> "1",
      "upperBound" -> "31",
      "numPartitions" -> spark.sparkContext.defaultParallelism.toString
    )

    // Custom query for reading specific columns and casting types, for partitioning by 'day_of_month'
    val query = "(select date, cast(day_of_month as int) from dates) as subq"

    // Read from JDBC using day_of_month-based partitioning and log the number of partitions
    printTime(
      "",
      printNumParts(
        "JDBC read partitioning by day_of_month",
        readDfFromJdbc(spark, query, jdbcReadByDayOfMonthOptions)
      )
    )
  }

}
