package cn.qphone.dmp.utils

/**
 * 处理指标
 */
object RtbUtils {

	/**
	 * 展示和点击
	 */
	def shows(requestmode: Int, iseffective: Int):List[Double] = {
		if (requestmode == 2 && iseffective == 1) List[Double](1, 0)
		else if (requestmode == 3 && iseffective == 1) List[Double](0, 1)
		else List[Double](0, 0)
	}

	/**
	 * 处理参与竞价，竞价成功，广告消费、广告成本
	 * 1:参与竞价数
	 * 2.竞价成功数
	 * 3.DSP广告消费
	 * 4.DSP广告成本
	 */
	def adPrice(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int, adorderid: Int, winprice:Double, adpayment:Double):List[Double] = {
		if (iseffective == 1 && isbilling == 1) { // 满足这个条件说明所有的指标都可能有
			if (iswin == 1) { // DSP广告消费、DSP广告成本肯定有
				if (adorderid != 0) { // 竞价成功数有
					if(isbid == 1) List(1, 1, winprice / 1000, adpayment / 1000) // 参与竞价数有
					else List(0, 1, winprice / 1000, adpayment / 1000) // 参与竞价数没有
				}else {// 竞价成功数没有
					if(isbid == 1) List(1, 0, winprice / 1000, adpayment / 1000)// 参与竞价数有
					else List(0, 0, winprice / 1000, adpayment / 1000)
				}
			}else { // DSP广告消费、DSP广告成本肯定、竞价成功数肯定没有
				if(isbid == 1) List(1, 0, 0, 0) // 参与竞价数有
				else List(0, 0, 0, 0)
			}
		}else List(0, 0, 0, 0) //满足这个条件说明所有的指标都肯定没有
	}


	/**
	 * 处理请求指标
	 */
	def requestAd(requestmode: Int, processnode: Int): List[Double] = {
		/*
		 * List的元素:第一个元素表示原始请求
		 * 第二个元素表示有效请求
		 * 第三个元素表示广告请求
		 * 1表示真，0表示假
		 */
		if (requestmode == 1 && processnode == 1) List[Double](1, 0 ,0)
		else if(requestmode == 1 && processnode == 2) List[Double](1, 1 ,0)
		else if(requestmode == 1 && processnode == 3) List[Double](1, 1 ,1)
		else List[Double](0, 0, 0)
	}
}