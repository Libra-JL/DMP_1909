package cn.qphone.dmp.context

import java.util.Properties

import cn.qphone.dmp.app.LocationRpt
import cn.qphone.dmp.utils.TagsUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
 * @author lijie
 */
/**
 * 标签上下文
 */
object TagsContext {
	def main(args: Array[String]): Unit = {
		if (args == null || args.length != 4) {
			println("Usage:<input><output><dic>")
			System.exit(-1)
		}

		val Array(input, output, dic1, dic2) = args

		val properties = new Properties()
		properties.load(LocationRpt.getClass.getClassLoader.getResourceAsStream("spark.properties"))


		val session = new SparkSession.Builder()
			.master("local")
			.appName(LocationRpt.getClass.getSimpleName)
			.getOrCreate()

		session.sqlContext.setConf(properties)

		//3. 获取到字典(url,app)
		val dicMap = session.sparkContext.textFile(dic1)
			.map(_.split("\t", -1)).filter(_.length >= 5)
			.map(arr => (arr(4), arr(1))).collectAsMap()

		//关键字字典
		val keyWordMap = session.sparkContext.textFile(dic2).map((_, 0)).collectAsMap()


		//4. 广播变量
		val broadcastDic: Broadcast[collection.Map[String, String]] = session.sparkContext.broadcast(dicMap)

		val keyWordMapBroadcast = session.sparkContext.broadcast(keyWordMap)

		//5. 读取日志数据
		val df = session.read.parquet(input)

		//6. 处理数据，打标签
		df.filter(TagsUtils.uuid).rdd.map(row => { // 保证有一个不为空的用户id
			//6.1 获取userid
			val userid = TagsUtils.getAnyOneUserId(row)
			//6.2 广告标签
			val adTag = TagsAd.makeTags(row)
			val appTag = TagsApp.makeTags(row, broadcastDic)
			val PlatformTag = TagsPlatform.makeTags(row)
			val DevTags = TagsDev.makeTags(row)
			val keyWordTags = TagsKeyWord.makeTags(row, keyWordMapBroadcast)
			val ZoneTags = TagsZone.makeTags(row)
			(ZoneTags)
		}).foreach(println)


	}

}
