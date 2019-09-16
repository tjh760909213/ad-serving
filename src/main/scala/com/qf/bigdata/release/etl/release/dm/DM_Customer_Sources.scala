package com.qf.bigdata.release.etl.release.dm

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.etl.release.dm.DM_Exposure_Cube.{handleReleaseJob, logger}
import com.qf.bigdata.release.etl.release.dw.{DWReleaseColumnsHelper, DWReleaseCustomer}
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * 投放目标客户数据集市
  */
object DM_Customer_Sources {
  // 日志处理
  val logger: Logger = LoggerFactory.getLogger(DM_Customer_Sources.getClass)

  /**
    * 目标客户
    * status
    */
  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String): Unit ={

    try{
      import spark.implicits._
      import org.apache.spark.sql.functions._
      //缓存级别
      val saveMode:SaveMode = ReleaseConstant.DEF_SAVEMODE
      val storageLevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
      //获取日志数据
      val customerClolumns: ArrayBuffer[String] = DM_Customer_Sources_Helper.selectDMCustomerSourceColumns()
      //处理当天数据(条件)
      val cutomerCondition: Column = $"${ReleaseConstant.DEF_PARTITION}" === lit(bdp_day)
      val customerReleaseDF: DataFrame = SparkHelper.readTableData ( spark, ReleaseConstant.DW_RELEASE_CUSTOMER )
        .where ( cutomerCondition )
        .selectExpr ( customerClolumns: _* )
        .persist ( storageLevel )

      println("查看数据===========================")
      customerReleaseDF.show(10,false)

      //统计渠道指标
      //Seq[Column]("sources","channels", "device_type")

      //插入表的select语句

      //按照条件进行聚合数据
      val customerSourceDMDF= customerReleaseDF.groupBy("sources","channels", "device_type").agg(
        // lit() =>获取字面量
        countDistinct("idcard").alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}"),
        count(lit((ReleaseConstant.COL_RELEASE_USER_COUNT)).alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}"))
      )
        .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))


      //将结果存储
      SparkHelper.writeTableData(customerSourceDMDF,ReleaseConstant.DM_CUSTOMER_SOURCES,saveMode)


      // 目标客户多维统计
      val customerCubeGroupColumns =Seq[Column](
        $"${ReleaseConstant.COL_RELEASE_SOURCES}",
        $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
        $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
        $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}",
        $"${ReleaseConstant.COL_RELEASE_GENDER}",
        $"${ReleaseConstant.COL_RELEASE_AREA_CODE}"
      )

      //插入表的select语句
//      val customerCubeColumns: ArrayBuffer[String] = DM_Customer_Sources_Helper.selectDMCustomerCubeColumns()
      //val customerCubeColmuns: ArrayBuffer[String] = DM_Customer_Sources_Helper.selectDMCustomerCubeColumns()
      //处理数据、聚合指标
      //val cubeDF: DataFrame = SparkHelper.readTableData(spark,ReleaseConstant.DM_CUSTOMER_CUBE,customerCubeColmuns)


      val cube: DataFrame = customerReleaseDF
        .groupBy ( "sources", "channels", "device_type", "age_range","gender", "area_code" )
        .agg (
          count ( "age_range" ),
          count ( "*" )
        )
        .withColumn ( s"${ReleaseConstant.DEF_PARTITION}", lit ( bdp_day ) )

        //.selectExpr(selectclo:_*)



      SparkHelper.writeTableData(cube,ReleaseConstant.DM_CUSTOMER_CUBE,saveMode)
    }catch{
      case ex:Exception=>{
        logger.error(ex.getMessage,ex)
      }
    }
  }

  def handleJobs(appName:String,bdp_day_begin:String,bdp_day_end:String): Unit ={
    var spark :SparkSession = null
    try{
      // spark 配置参数
      val conf = new SparkConf()
        .set("hive.exec.dynamic.partition", "true")
        .set("hive.exec.dynamic.partition.mode", "nonstrict")
        .set("spark.sql.shuffle.partitions", "32")
        .set("hive.merge.mapfiles", "true")
        .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
        .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
        .set("spark.sql.crossJoin.enabled", "true")
        //.set("spark.sql.warehouse.dir","hdfs://hdfsCluster/sparksql/db")
        .setAppName(appName)
        .setMaster("local[4]")
      // spark  上下文
      spark = SparkHelper.createSpark(conf)
      // 参数校验
      val timeRanges = SparkHelper.rangeDates(bdp_day_begin,bdp_day_end)
      for (bdp_day <- timeRanges.reverse){
        val bdp_date = bdp_day.toString
        handleReleaseJob(spark,appName,bdp_date)
      }
    }catch {
      case ex :Exception=>{
        logger.error(ex.getMessage,ex)
      }
    }finally {
      if(spark != null){
        spark.stop()
      }
    }
  }


  def main(args: Array[String]): Unit = {
    val appName = "s"
    val bdp_day_begin:String ="20190909"
    val bdp_day_end:String ="20190909"
    // 执行Job
    handleJobs(appName,bdp_day_begin,bdp_day_end)
  }

}
