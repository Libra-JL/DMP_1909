package cn.qphone.dmp.etl

import java.util.Properties

import cn.qphone.dmp.traits.LoggerTrait
import cn.qphone.dmp.utils.{CommonUtils, SchemaUtils, SparkUnits}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Log2Parquet extends LoggerTrait {
	val SPARK_PROPERTIES: String = "spark.properties"

	def main(args: Array[String]): Unit = {
		//1. 控制读取和存储的参数
		if (args == null || args.length != 2) {
			println("Usage : <input> <output>")
			System.exit(-1)
		}
		val Array(input, output) = args
		//2. 获取到入口并配置序列化以及压缩方式
		val properties = new Properties()
		properties.load(Log2Parquet.getClass.getClassLoader.getResourceAsStream(SPARK_PROPERTIES))
		val spark = SparkUnits.getLocalSparkSession("Log2Parquet")
		spark.sqlContext.setConf(properties)
		//3. 读取数据
		val lines: RDD[String] = spark.sparkContext.textFile(input)
		//4. 切分数据，对RDD中的数据赋予元数据信息
		val rdd = lines.map(_.split(",", -1)).filter(_.length >= 85).map(arr => {
			Row(
				arr(0),
				CommonUtils.toInt(arr(1)),
				CommonUtils.toInt(arr(2)),
				CommonUtils.toInt(arr(3)),
				CommonUtils.toInt(arr(4)),
				arr(5),
				arr(6),
				CommonUtils.toInt(arr(7)),
				CommonUtils.toInt(arr(8)),
				CommonUtils.toDouble(arr(9)),
				CommonUtils.toDouble(arr(10)),
				arr(11),
				arr(12),
				arr(13),
				arr(14),
				arr(15),
				arr(16),
				CommonUtils.toInt(arr(17)),
				arr(18),
				arr(19),
				CommonUtils.toInt(arr(20)),
				CommonUtils.toInt(arr(21)),
				arr(22),
				arr(23),
				arr(24),
				arr(25),
				CommonUtils.toInt(arr(26)),
				arr(27),
				CommonUtils.toInt(arr(28)),
				arr(29),
				CommonUtils.toInt(arr(30)),
				CommonUtils.toInt(arr(31)),
				CommonUtils.toInt(arr(32)),
				arr(33),
				CommonUtils.toInt(arr(34)),
				CommonUtils.toInt(arr(35)),
				CommonUtils.toInt(arr(36)),
				arr(37),
				CommonUtils.toInt(arr(38)),
				CommonUtils.toInt(arr(39)),
				CommonUtils.toDouble(arr(40)),
				CommonUtils.toDouble(arr(41)),
				CommonUtils.toInt(arr(42)),
				arr(43),
				CommonUtils.toDouble(arr(44)),
				CommonUtils.toDouble(arr(45)),
				arr(46),
				arr(47),
				arr(48),
				arr(49),
				arr(50),
				arr(51),
				arr(52),
				arr(53),
				arr(54),
				arr(55),
				arr(56),
				CommonUtils.toInt(arr(57)),
				CommonUtils.toDouble(arr(58)),
				CommonUtils.toInt(arr(59)),
				CommonUtils.toInt(arr(60)),
				arr(61),
				arr(62),
				arr(63),
				arr(64),
				arr(65),
				arr(66),
				arr(67),
				arr(68),
				arr(69),
				arr(70),
				arr(71),
				arr(72),
				CommonUtils.toInt(arr(73)),
				CommonUtils.toDouble(arr(74)),
				CommonUtils.toDouble(arr(75)),
				CommonUtils.toDouble(arr(76)),
				CommonUtils.toDouble(arr(77)),
				CommonUtils.toDouble(arr(78)),
				arr(79),
				arr(80),
				arr(81),
				arr(82),
				arr(83),
				CommonUtils.toInt(arr(84))

			)
		})
		//5. 构建DataFrame
		val df = spark.createDataFrame(rdd, SchemaUtils.logStructType)
		//6. 存储成parquet的格式
		df.write.parquet(output)
		//7. 释放资源
		SparkUnits.stop(spark)
	}
}