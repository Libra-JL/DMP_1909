package cn.qphone.dmp.app

import java.util.Properties

import cn.qphone.dmp.traits.LoggerTrait
import cn.qphone.dmp.utils.SparkUnits
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object TerminalEquipment extends LoggerTrait {
	def main(args: Array[String]): Unit = {
		if (args == null || args.length != 2) {
			println("Usage:<input><output>")
			System.exit(-1)
		}

		val Array(input, output) = args

		val properties = new Properties()
		properties.load(LocationRpt.getClass.getClassLoader.getResourceAsStream("spark.properties"))


		val session = new sql.SparkSession.Builder()
			.master("local")
			.appName(LocationRpt.getClass.getSimpleName)
			.getOrCreate()

		val unit = session.sqlContext.setConf(properties)

		//加载数据

		val frame = session.read.parquet(input)

		frame.createOrReplaceTempView("log")

		val result = session.sql(
			"""
			  |select ispname,
			  |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) ys_request_cnt,
			  |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) yx_request_cnt,
			  |sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) ad_request_cnt,
			  |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) cy_bid_cnt,
			  |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) bid_succ_cnt,
			  |sum(case when iseffective = 1 and requestmode = 2 then 1 else 0 end) show_cnt,
			  |sum(case when iseffective = 1 and requestmode = 3 then 1 else 0 end) click_cnt,
			  |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice / 1000 else 0.0 end) price_cnt,
			  |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment / 1000 else 0.0 end) ad_cnt
			  |from log
			  |group by ispname
			  |""".stripMargin)
		//		result.show()
		//sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end)/sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) bid_succ_rate,
		//sum(case when iseffective = 1 and requestmode = 2 then 1 else 0 end)/sum(case when iseffective = 1 and requestmode = 3 then 1 else 0 end) click_rate,

		result.write.partitionBy("ispname").save(output)

		session.stop()


	}
}
