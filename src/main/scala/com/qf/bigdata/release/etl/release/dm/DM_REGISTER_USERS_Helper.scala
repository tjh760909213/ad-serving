package com.qf.bigdata.release.etl.release.dm

import scala.collection.mutable.ArrayBuffer

object DM_REGISTER_USERS_Helper {
  def selectDWReleaseColumns():ArrayBuffer[String]={
    var columns = new ArrayBuffer[String]()
    columns.+=("device_type")
    columns.+=("sources  ")
    columns.+=("channels  ")
    columns.+=("bdp_day")


  }
}
