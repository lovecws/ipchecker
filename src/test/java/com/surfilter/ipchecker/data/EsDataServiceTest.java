package com.surfilter.ipchecker.data;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;

public class EsDataServiceTest {

	@Test
	public void getIps() {
		EsDataService esDataService = new EsDataService("http://172.31.134.229:9200", "wscan", "base");

		String filePath = System.getProperty("user.dir").replace("\\", "/");
		filePath = filePath + "/event/jiangxi";
		File eventDir = new File(filePath);
		if (!eventDir.exists()) {
			eventDir.mkdirs();
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String dateString = sdf.format(new Date());
		String sourcePath = filePath + "/ip" + dateString + ".txt";
		esDataService.getIps("location.region.zh-CN.untouched", "江西", 1000, sourcePath);
	}
}
