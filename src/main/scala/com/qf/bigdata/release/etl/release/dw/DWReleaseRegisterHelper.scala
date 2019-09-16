package com.qf.bigdata.release.etl.release.dw

import scala.collection.mutable.ArrayBuffer

object DWReleaseRegisterHelper {
  def selectDWReleaseColumns():ArrayBuffer[String]={
    var columns = new ArrayBuffer[String]()
    columns.+=("get_json_object(exts,'$.user_register') as user_register ")
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num ")
    columns.+=("device_type")
    columns.+=("sources  ")
    columns.+=("channels  ")
    columns.+=("ct")
    columns.+=("bdp_day")


  }
}
