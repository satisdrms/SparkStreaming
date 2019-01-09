package com.rnexamples.utils


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io._

object HdfsUtils {
  
  def deleteFilesInHDFS(path: String) = {
      val filePath = new Path(path)
      val conf = new Configuration()
      conf.set("fs.defaultFS", "hdfs://sandbox-hdp.hortonworks.com:8020")
      val HDFSFilesSystem= FileSystem.get(conf)
      //val HDFSFilesSystem = filePath.getFileSystem(sc.hadoopConfiguration)
      if (HDFSFilesSystem.exists(filePath)) {
        HDFSFilesSystem.delete(filePath, true)
      }
  }
  
  def createFilesInHDFS(keyPath: String,value: String) = {
      val filePath = new Path(keyPath)
      val conf = new Configuration()
      conf.set("fs.defaultFS", "hdfs://sandbox-hdp.hortonworks.com:8020")
      val fs= FileSystem.get(conf)
      val output = fs.create(filePath)
      val writer = new PrintWriter(output)
      try {
        writer.write(value) 
        writer.write("\n")
        }
      finally 
      {
        writer.close()
        println("Closed!")
      }
}
  
  def putKeyValToHDFS(path: String,key: String,value: String) = {
      val keyPath=path.concat("/").concat(key).concat("/")
      val filePath = new Path(keyPath)
      deleteFilesInHDFS(keyPath)
      createFilesInHDFS(keyPath,value)
      }
  
  def getKeyFromHDFS(path: String,key: String): String  = {
      val keyPath=path.concat("/").concat(key)
      val filePath = new Path(keyPath)
      val conf = new Configuration()
      conf.set("fs.defaultFS", "hdfs://sandbox-hdp.hortonworks.com:8020")
      val fs= FileSystem.get(conf)
      scala.io.Source.fromInputStream(fs.open(filePath)).mkString
      }
}