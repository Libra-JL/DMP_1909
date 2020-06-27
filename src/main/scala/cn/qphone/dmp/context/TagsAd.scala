package cn.qphone.dmp.context

import cn.qphone.dmp.traits.TagsTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object TagsAd extends TagsTrait{
	/**
	 * 打广告标签方法
	 */
	override def makeTags(args: Any*): List[(String, Int)] = {
		//1. 定义个序列接收结果
		var list = List[(String, Int)]()
		//2. 解析参数
		val row = args(0).asInstanceOf[Row]
		//3. 获取广告类型和名称
		val adType = row.getAs[Int]("adspacetype")
		val adName = row.getAs[String]("adspacetypename")

		//4. 处理补0
		adType match {
			case v if v > 9 => list:+=("LC"+v,1)
			case v if v > 0 && v <= 9 => list:+=("LC0"+v,1)
		}

		//5. 拼凑标签
		if (StringUtils.isNoneBlank(adName)) {
			list:+=("LN"+ adName, 1)
		}
		list
	}
}