package cn.qphone.dmp.context

import cn.qphone.dmp.traits.TagsTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsApp extends TagsTrait {
	override def makeTags(args: Any*): List[(String, Int)] = {
		var list = List[(String, Int)]()
		val row = args(0).asInstanceOf[Row]
		val map: Broadcast[collection.Map[String, String]] = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]

		val appName = row.getAs[String]("appname")
		val appId = row.getAs[String]("appid")

		if (StringUtils.isNoneBlank(appName)) {
			list :+= ("APP" + appName, 1)
		} else {
			list :+= ("APP" + map.value.getOrElse(appId, "其他"), 1)
		}
		list
	}
}
