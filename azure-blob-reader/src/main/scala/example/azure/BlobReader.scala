package example.azure

object BlobReader extends App {

  val accountName = "sparkblobpoc"
  val containerName = "pricing"

  val spark = SparkUtils.createSparkSession()

  try {
    SparkUtils.readJsonFilesFromBlob(spark, accountName, containerName)
  } finally {
    spark.stop()
  }
}
