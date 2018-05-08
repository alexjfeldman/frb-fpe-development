sudo spark-shell --jars ../target/scala-2.11/encryption_2.11-0.0.1.jar,format-preserving-encryption-1.0.0.jar,format-preserving-encryption-1.0.0-javadoc.jar,format-preserving-encryption-1.0.0-sources.jar

import org.frb.encryption._

var AESEncryptionMethod = "86753090867530908675309086753090".getBytes()

val inputDF = spark.read.format("text").load("/user/hadoop/FSIWest.pem")

val PrivateKeyHolder = inputDF.rdd.map(x=>x.mkString).collect

var FirstRunBoolean = false
var FirstRunBoolean = true
var iterator = 2
var outputString = ""
var intrimString = ""
var startString = ""

while(iterator <= PrivateKeyHolder.length-1) {
  if(FirstRunBoolean) {
    outputString=PrivateKeyHolder(iterator-1)
    FirstRunBoolean=false
  } else {
    startString=PrivateKeyHolder(iterator-1)
    intrimString=outputString+startString
    outputString=intrimString
  }
  iterator = iterator + 1
}


//Make the strings into binary keys
val BinaryKey = outputString.getBytes
val OtherBinaryKey = "Its another key".getBytes()

//persist the inputDF
val inputDF = spark.read.format("csv").option("header","true").load("s3://healthcaretoydata/Patient_Information.csv").limit(50).persist
inputDF.count


val encryptedDF = EncryptionUtilities.FPEMasking(inputDF, "Patient_ID", "encrypt", "all", AESEncryptionMethod, BinaryKey)

val decryptedDF = EncryptionUtilities.FPEMasking(encryptedDF, "Patient_ID", "decrypt", "all", AESEncryptionMethod, BinaryKey)
val wrongDecryptedDF  = EncryptionUtilities.FPEMasking(encryptedDF, "Patient_ID", "decrypt", "all", AESEncryptionMethod, OtherBinaryKey)

inputDF.show
encryptedDF.show
decryptedDF.show
wrongDecryptedDF.show

val encryptedDF2 = EncryptionUtilities.FPEMasking(encryptedDF, "Social_Security_Number", "encrypt", "numeric", AESEncryptionMethod, BinaryKey)
encryptedDF2.show