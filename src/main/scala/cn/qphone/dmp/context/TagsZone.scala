package cn.qphone.dmp.context

import cn.qphone.dmp.traits.TagsTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object TagsZone extends TagsTrait{
	override def makeTags(args: Any*): List[(String, Int)] = {
		var list = List[(String, Int)]()

		val row = args(0).asInstanceOf[Row]

		val proveniceName = row.getAs[String]("provincename")
		val cityName = row.getAs[String]("cityname")


		if (StringUtils.isNoneBlank(proveniceName)){
			list:+=("ZP"+proveniceName,1)
		}
		if (StringUtils.isNoneBlank(cityName)){
			list:+=("ZC"+cityName,1)
		}

		list
	}
}
