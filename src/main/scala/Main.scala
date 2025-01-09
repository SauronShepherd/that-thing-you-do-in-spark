import Utils._

object Main {

  def main(args: Array[String]): Unit = {

    // Initialize Spark session
    val spark = createSparkSession()

    // Generating Data
    printSection("Generating Data")
    val df = GeneratingData.run(spark)

    // Filtering Data
    printSection("Filtering Data")
    val (filteredDF, filteredRepDF, filteredCoaDF) = FilteringData.run(df)

    // Grouping Data
    printSection("Grouping Data")
    GroupingData.run(df, filteredDF, filteredRepDF, filteredCoaDF)

    // Joining Data
    printSection("Joining Data")
    val multiJoinDF = JoiningData.run(df, filteredCoaDF)

    // Writing & Reading using files
    printSection("Writing & Reading using files")
    val readFilesDF =WritingReadingFiles.run(multiJoinDF)

    // Writing & Reading using JDBC
    printSection("Writing & Reading using JDBC")
    WritingReadingJDBC.run(readFilesDF)

    // Stop Spark session
    spark.stop()
  }

}
