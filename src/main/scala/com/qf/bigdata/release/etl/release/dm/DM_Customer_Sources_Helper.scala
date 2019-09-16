package com.qf.bigdata.release.etl.release.dm


import scala.collection.mutable.ArrayBuffer

object DM_Customer_Sources_Helper {



  /**
    * 渠道指标
    */
  def selectDMCustomerSourceColumns():ArrayBuffer[String]={
    var columns = new ArrayBuffer[String]()

    columns.+= ( "release_session" )
    columns.+= ( "release_status" )
    columns.+= ( "device_num" )
    columns.+= ( "device_type" )
    columns.+= ( "sources" )
    columns.+= ( "channels" )
    columns.+= ( "idcard " )
    columns.+= ( "age " )
    //加载UDF函数
    columns.+= ( "getAgeRange(age) as age_range" )
    columns.+= ( "gender" )
    columns.+= ( "area_code" )
    columns.+= ( "aid" )
    columns.+= ( "ct" )
    columns.+= ( "bdp_day" )
    columns
  }

  /**
    * 目标客户多维统计
    */
  def selectDMCustomerCubeColumns():ArrayBuffer[String]={
    var columns = new ArrayBuffer[String]()

    columns.+= ( "sources" )
    columns.+= ( "channels" )
    columns.+= ( "device_type" )
    columns.+= ( "getAgeRange(age) as age_range" )
    columns.+= ( "gender" )
    columns.+= ( "area_code" )
    columns.+= ( "bdp_day" )


  }


}