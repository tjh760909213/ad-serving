package com.qf.bigdata.release.etl.release.dw

import com.qf.bigdata.release.constant.ReleaseConstant
import com.qf.bigdata.release.enums.ReleaseStatusEnum
import com.qf.bigdata.release.etl.release.dw.DWReleaseCustomer.{handleJobs, handleReleaseJob, logger}
import com.qf.bigdata.release.util.SparkHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

/**
  * DW 曝光主题
  */
object DWReleaseExposure {
  // 日志处理
  val logger: Logger = LoggerFactory.getLogger(DWReleaseCustomer.getClass)

  /**
    * 目标客户
    * status = "03"
    */
  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String): Unit ={
    try{
      // 导入隐式转换
      import spark.implicits._
      import org.apache.spark.sql.functions._
      // 设置缓存级别
      val storageLevel :StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode :SaveMode = ReleaseConstant.DEF_SAVEMODE
      // 获取当天日志字段数据
      val exposureColumns: ArrayBuffer[String] = DWReleaseExposureHelper.selectDWReleaseColumns()
      // 当天数据，设置条件，根据条件进行查询，后续调用数据
      val exposureReleaseCondition =
        (col(s"${ReleaseConstant.DEF_PARTITION}")) === lit(bdp_day) and
          col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}") ===
            lit(ReleaseStatusEnum.REGISTER.getCode)
      // 填入条件
      //这个DF就是读取ods表数据之后的
      val exposureReleaseDF: DataFrame = SparkHelper
        .readTableData(spark,ReleaseConstant.ODS_RELEASE_SESSION,exposureColumns)
        // 查询条件
        .where(exposureReleaseCondition)
        // 重分区
        .repartition(ReleaseConstant.DEF_SOURCE_PARTITIONS)
      println("查询结束======================结果显示")
      exposureReleaseDF.show(10,false)
      // 目标用户
      SparkHelper.writeTableData(exposureReleaseDF,ReleaseConstant.DW_RELEASE_EXPOSURE,saveMode)

    }catch {
      // 错误信息处理
      case ex:Exception =>{
        logger.error(ex.getMessage,ex)
      }
    }
  }

  /**
    * 曝光主题
    */
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
    // 如果没有Windows下的hadoop环境变量的话，需要内部执行，自己加载，如果有了，那就算了
    //System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    val appName :String = "dw_release_register_job"
    val bdp_day_begin:String ="20190909"
    val bdp_day_end:String ="20190909"
    // 执行Job
    handleJobs(appName,bdp_day_begin,bdp_day_end)
  }

}
