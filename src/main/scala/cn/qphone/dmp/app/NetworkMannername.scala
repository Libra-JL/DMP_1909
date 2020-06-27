package cn.qphone.dmp.app

import java.util.Properties

import cn.qphone.dmp.traits.LoggerTrait
import cn.qphone.dmp.utils.RtbUtils
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.util.StringUtils

object NetworkMannername extends LoggerTrait {
	def main(args: Array[String]): Unit = {
		if (args == null || args.length != 2) {
			println("Usage:<input><output>")
			System.exit(-1)
		}

		val Array(input, output, dic) = args

		val properties = new Properties()
		properties.load(LocationRpt.getClass.getClassLoader.getResourceAsStream("spark.properties"))


		val session = new sql.SparkSession.Builder()
			.master("local")
			.appName(LocationRpt.getClass.getSimpleName)
			.getOrCreate()

		val unit = session.sqlContext.setConf(properties)

		//加载数据
		val dicMap = session.sparkContext.textFile(dic)
			.map(_.split("\t", -1))
			.filter(_.length >= 5)
			.map(
				arr =>
					(arr(4), arr(1))
			).collectAsMap()

		val broadDic = session.sparkContext.broadcast(dicMap)

		val logDF = session.read.parquet(input)

		logDF.rdd.map(row=>{
			val appname = row.getAs[String]("appname")
			if (appname!=null || appname!=""){
				broadDic.value.getOrElse(row.getAs[String]("appname"),"其他")
			}
			val requestmode:Int = row.getAs[Int]("requestmode")
			val processnode = row.getAs[Int]("processnode")
			val iseffective = row.getAs[Int]("iseffective")
			val isbilling = row.getAs[Int]("isbilling")
			val isbid = row.getAs[Int]("isbid")
			val iswin = row.getAs[Int]("iswin")
			val adorderid = row.getAs[Int]("iswin")
			val winprice = row.getAs[Double]("winprice")
			val adpayment = row.getAs[Double]("adpayment")

			val doubles = RtbUtils.requestAd(requestmode, processnode)
//			val doubles = RtbUtils.requestAd(iseffective, isbilling,isbid,iswin,adorderid)
//			val doubles = RtbUtils.requestAd(requestmode, processnode)
		})


		logDF.createOrReplaceTempView("log")


		val result = session.sql(
			"""
			  |select networkmannername,
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
			  |group by networkmannername
			  |""".stripMargin)
		result.show()
		//sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end)/sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) bid_succ_rate,
		//sum(case when iseffective = 1 and requestmode = 2 then 1 else 0 end)/sum(case when iseffective = 1 and requestmode = 3 then 1 else 0 end) click_rate,

		result.write.partitionBy("networkmannername").save(output)

		session.stop()


	}
}
