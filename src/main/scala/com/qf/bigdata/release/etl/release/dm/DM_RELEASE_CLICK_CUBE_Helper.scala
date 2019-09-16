package com.qf.bigdata.release.etl.release.dm

import scala.collection.mutable.ArrayBuffer

object DM_RELEASE_CLICK_CUBE_Helper {
  def selectDWReleaseColumns():ArrayBuffer[String]={
    var columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("bdp_day")


  }
}
