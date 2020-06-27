package cn.qphone.dmp.context

import cn.qphone.dmp.traits.TagsTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsPlatform extends TagsTrait {
	override def makeTags(args: Any*): List[(String, Int)] = {
		var list = List[(String, Int)]()
		val row = args(0).asInstanceOf[Row]
		val adplatformproviderid = row.getAs[Int]("adplatformproviderid")

		if (StringUtils.isNoneBlank(adplatformproviderid.toString)) {
			list :+= ("CN" + adplatformproviderid, 1)
		}
		list
	}
}
