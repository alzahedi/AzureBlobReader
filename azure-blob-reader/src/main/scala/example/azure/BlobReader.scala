package example.azure

object BlobReader extends App {

  val accountName = "sparkblobpoc"
  val containerName = "pricing"
  val blobName = "ComputePricing.json"

  val spark = SparkUtils.createSparkSession()

  try {
    val sasToken = BlobService.generateUserDelegationSas(
      accountName,
      containerName,
      blobName
    )
    val blobUrl =
      s"wasbs://$containerName@$accountName.blob.core.windows.net/$blobName"

    println(s"Blob URL: $blobUrl")
    SparkUtils.readJsonFromBlob(
      spark,
      blobUrl,
      sasToken,
      accountName,
      containerName
    )
  } finally {
    spark.stop()
  }
}
