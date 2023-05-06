// Databricks notebook source
import org.apache.spark.sql.SparkSession
import java.net.URI
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import java.net.URI
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.io.IOUtils

// COMMAND ----------

var mountPoint = "/mnt/project"
val hadoopConfig = new Configuration()


// COMMAND ----------

val news_data = spark.sparkContext.textFile(mountPoint+"/news_data.csv")
val template = spark.sparkContext.textFile(mountPoint+"/template.csv")

// COMMAND ----------

// Mount point for project folder

var counter = 0
template.collect().foreach(a => {
    val arrBuf = ArrayBuffer[String]()
    counter +=1
    news_data.collect().foreach(e => {
      val x = e.lastIndexOf(',')
      val res = a.replace("[x]", (e.substring(0,x)).toLowerCase())
      val res2 =  res.replace("[y]", (e.substring(x+1, e.length).toLowerCase()).trim())
      val res1 =  res2.replaceAll("[\\[\\]]", "")      
      arrBuf += s"$res1"
    })

    val txtPath = new URI(s"$mountPoint/output/template_$counter.txt")
    val fs = FileSystem.get(txtPath, hadoopConfig)

    val txtString = arrBuf.map(_.mkString("")).mkString("\n")
    val txtFilePath = new Path(txtPath)
    val txtFile = fs.create(txtFilePath)    

    try {
      val outputStream = txtFile.write(txtString.getBytes)      
    } finally {
      IOUtils.closeStream(txtFile)
    }
  })

// COMMAND ----------


