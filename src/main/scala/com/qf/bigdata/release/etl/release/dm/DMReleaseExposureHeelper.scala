package com.qf.bigdata.release.etl.release.dm

import scala.collection.mutable.ArrayBuffer

object DMReleaseExposureHeelper {
  def selectDWReleaseColumns():ArrayBuffer[String]={
    var columns = new ArrayBuffer[String]()

    columns.+=("release_session")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")

    columns.+=("ct")
    columns.+=("bdp_day")
    columns


  }
}
