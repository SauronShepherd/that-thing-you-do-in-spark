import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}
import Utils._

object GeneratingData {

  /**
   * Generates a DataFrame with dates for the current year and adds calculated columns for month and day of the month.
   *
   * @param spark The Spark session used for creating DataFrames.
   * @return A DataFrame containing dates from the start to the end of the current year, with calculated month and day columns.
   */
  def run(spark: SparkSession): DataFrame = {

    // Get the start and end dates for the current year
    val (startDate, endDate) = getStartEndDates

    // Create a list of all dates within the given range (startDate to endDate)
    val dates = (0 until java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate).toInt)
      .map(startDate.plusDays(_))

    // Import implicits to use DataFrame creation methods
    import spark.implicits._

    // Create the DataFrame from the list of dates and add calculated columns: 'month' and 'day_of_month'
    val df = printNumParts(
      "Generated DataFrame with date details",
      dates.toDF("date")
        .withColumn("month", date_format(col("date"), "MMMM"))      // Extract the full month name
        .withColumn("day_of_month", date_format(col("date"), "dd")) // Extract the day of the month as a 2-digit number
    )

    df
  }

}
