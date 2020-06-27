package cn.qphone.dmp.context

import cn.qphone.dmp.traits.TagsTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsKeyWord extends TagsTrait {
	override def makeTags(args: Any*): List[(String, Int)] = {
		var list = List[(String, Int)]()

		val row = args(0).asInstanceOf[Row]
		val map = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]
		val kw = row.getAs[String]("keywords").split("\\|")
		kw.filter(
			word => word.length > 3 && word.length < 8 && map.value.contains(word)
		).foreach(word => {
			if(StringUtils.isNoneBlank(word)){
				list :+= ("k" + word, 1)
			}
		})
		list

	}
}
