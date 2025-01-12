import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

object Utils {

  def createSparkSession(config: Map[String, String] = Map()): SparkSession= {
    SparkSession.builder()
      .master("local[*]")
      .config(config)
      .getOrCreate()
  }

  /**
   * Calculates the start and end dates for the current year.
   *
   * @return A tuple containing:
   *         - The start date (January 1st of the current year).
   *         - The end date (January 1st of the next year).
   */
  def getStartEndDates: (LocalDate, LocalDate) = {
    val currentYear = LocalDate.now().getYear
    val startDate = LocalDate.of(currentYear, 1, 1)
    val endDate = LocalDate.of(currentYear + 1, 1, 1)
    (startDate, endDate)
  }

  /**
   * Prints a section title within a decorated box for better visibility.
   *
   * @param text    The title text.
   */
  def printSection(text: String): Unit = {
    val boxChar = '*'
    val border = boxChar.toString * (text.length + 4)
    println()
    println(border)
    println(s"$boxChar $text $boxChar")
    println(border)
  }

  /**
   * Prints the number of partitions and row count of a DataFrame.
   *
   * @param text Description of the DataFrame.
   * @param df   The DataFrame to analyze.
   * @return The input DataFrame (unchanged).
   */
  def printNumParts(text: String, df: DataFrame): DataFrame = {
    val numPartitions = df.rdd.getNumPartitions
    val rowCount = df.cache().count()
    println(s"$text => numPartitions: $numPartitions - rowCount: $rowCount")
    df
  }

  /**
   * Measures and prints the execution time of a block of code.
   *
   * @param block The code block to execute.
   * @return The result of the execution
   */
  def printTime(text: String, block: => DataFrame): DataFrame = {
    val startTime = System.nanoTime()
    val result = block
    val endTime = System.nanoTime()
    val durationSeconds = (endTime - startTime) / 1e9
    println(s"$text => Executed in $durationSeconds s")
    result
  }

  /**
   * Generates JDBC connection options, merging default values with provided ones.
   *
   * @param dbtable The table name to connect to.
   * @param options Additional connection options.
   * @return A map of JDBC connection options.
   */
  private def jdbcOptions(dbtable: String, options: Map[String, String]): Map[String, String] = {
    Map(
      "url" -> "jdbc:postgresql://localhost:5432/db",
      "user" -> "ps",
      "password" -> "ps",
      "driver" -> "org.postgresql.Driver",
      "dbtable" -> dbtable
    ) ++ options
  }

  /**
   * Writes a DataFrame to a JDBC destination.
   *
   * @param df      The DataFrame to write.
   * @param dbtable The destination table name.
   * @param options Additional write options.
   */
  def writeDfToJdbc(df: DataFrame, dbtable: String, options: Map[String, String] = Map()): DataFrame = {
    df.write.format("jdbc")
      .mode("overwrite")
      .options(jdbcOptions(dbtable, options))
      .save()
    df
  }

  /**
   * Reads a DataFrame from a JDBC source.
   *
   * @param spark   The SparkSession instance.
   * @param dbtable The source table name.
   * @param options Additional read options.
   * @return The DataFrame read from JDBC.
   */
  def readDfFromJdbc(spark: SparkSession, dbtable: String, options: Map[String, String] = Map()): DataFrame = {
    spark.read.format("jdbc")
      .options(jdbcOptions(dbtable, options))
      .load()
  }

}
