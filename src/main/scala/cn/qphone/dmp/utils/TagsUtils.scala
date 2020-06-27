package cn.qphone.dmp.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsUtils {

	val uuid = "imei != '' or mac != '' or idfa != '' or openudid != '' or androidid != ''"

	def getAnyOneUserId(row:Row): Unit ={
		row match {
			case v if StringUtils.isNoneBlank(v.getAs[String]("imei")) => "IM:" + v.getAs[String]("imei")
			case v if StringUtils.isNoneBlank(v.getAs[String]("mac")) => "MC:" + v.getAs[String]("mac")
			case v if StringUtils.isNoneBlank(v.getAs[String]("idfa")) => "IDFA:" + v.getAs[String]("idfa")
			case v if StringUtils.isNoneBlank(v.getAs[String]("openudid")) => "OID:" + v.getAs[String]("openudid")
			case v if StringUtils.isNoneBlank(v.getAs[String]("androidid")) => "AID:" + v.getAs[String]("androidid")
		}
	}
}
