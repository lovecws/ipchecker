package com.surfilter.ipchecker.data;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.surfilter.ipchecker.StartUp;
import com.surfilter.ipchecker.entity.EventRecordEntity;
import com.surfilter.ipchecker.util.EventUtil;

/**
 * 从各种不同的来源获取到ip数据集合 将ip集合写入到文本文件中
 */
public abstract class BaseDataService {
	
	public static final Logger log = Logger.getLogger(StartUp.class);
	
	public abstract void extractData(String fieldName, String fieldValue, int size,String filePath);
	
	/**
	 * 将ip数据写入到文本文件中
	 * @param ips ip集合
	 * @param bufferedWriter
	 */
	public void ipRecord(List<String> ips,BufferedWriter bufferedWriter){
		try {
			for (String ip : ips) {
				log.info(ip);
				bufferedWriter.write(ip);
				bufferedWriter.newLine();
			}
		} catch (Exception e1) {
			log.error(e1);
		} finally {
			try {
				bufferedWriter.flush();
			} catch (IOException e) {
				log.error(e);
			}
		}
	}
}
