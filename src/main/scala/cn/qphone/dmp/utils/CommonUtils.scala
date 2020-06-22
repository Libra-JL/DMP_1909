package cn.qphone.dmp.utils

import java.util.Properties

import scala.collection.mutable

/**
 * 通用工具类
 */
object CommonUtils {
	/**
	 * 读取classpath路径下的指定的名称的properties文件，并将其数据转换位一个不可变map
	 */
	def toMap(propertyName:String) : Map[String, String] = {
		//1. 创建properties对象并读取配置
		val properties = new Properties()
		properties.load(CommonUtils.getClass.getClassLoader.getResourceAsStream(propertyName))
		//2. 封装到Map
		//2.1 获取到properties中的所有的key
		val map = mutable.Map[String, String]()
		val iterator = properties.stringPropertyNames().iterator()
		//2.2 遍历
		while(iterator.hasNext) {
			val key:String = iterator.next()
			val value:String = properties.getProperty(key)
			map.put(key, value)
		}
		val immap:Map[String, String] = map.toMap
		immap
	}

	def main(args: Array[String]): Unit = {
		val map = toMap("kafka.properties")
		println(map)
	}
}
