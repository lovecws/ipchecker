package com.surfilter.ipchecker.data;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.surfilter.ipchecker.util.HttpClientUtil;

/**
 * 从es中抽取数据
 * @author ganliang
 *
 */
public class EsDataService extends BaseDataService {

	public String ELASTICSEARCH_BASE_URL = null;
	public String ELASTICSEARCH_WSCAN_URL = null;
	public static final Logger log = Logger.getLogger(EsDataService.class);

	/**
	 * 使用滚动api来查询
	 * @param fieldName 查询字段名称
	 * @param fieldValue 查询字段值
	 * @param size 每次批量处理数量
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public void extractData(String fieldName, String fieldValue, int size, String filePath) {
		log.info("开始从es中抽取数据.................................................");
		// 使用scroll api调用 获取scroll
		String result = HttpClientUtil.get(ELASTICSEARCH_WSCAN_URL + "/_search?scroll=5m&search_type=scan&size=" + size
				+ "&q=" + fieldName + ":" + fieldValue);
		Map scrollResultMap = JSON.parseObject(result, Map.class);
		log.info(scrollResultMap);
		String _scroll_id = scrollResultMap.get("_scroll_id").toString();
		Map scrollHitMap = (Map) scrollResultMap.get("hits");
		log.info("total record:" + scrollHitMap.get("total"));

		BufferedWriter bufferedWriter = null;
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(filePath));
			while (true) {
				result = HttpClientUtil
						.get(ELASTICSEARCH_BASE_URL + "/_search/scroll?scroll=5m&scroll_id=" + _scroll_id);
				Map resultMap = JSON.parseObject(result, Map.class);
				_scroll_id = resultMap.get("_scroll_id").toString();
				List<String> ips = parseResult(resultMap);
				if (ips == null) {
					break;
				}
				ipRecord(ips, bufferedWriter);
			}
		} catch (Exception e) {
			log.error(e);
		} finally {
			try {
				bufferedWriter.flush();
				bufferedWriter.close();
			} catch (Exception e) {
			}
			// 关闭 scroll
			HttpClientUtil.delete(ELASTICSEARCH_BASE_URL + "/_search/scroll?scroll_id=" + _scroll_id);
			log.info("结束从es中抽取数据.................................................");
		}
	}
	
	/**
	 * 解析es返回结果
	 * @param resultMap
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List<String> parseResult(Map resultMap){
		Map hitMap = (Map) resultMap.get("hits");
		
		List<Map> records = (List<Map>) hitMap.get("hits");
		if (records.size() == 0) {
			return null;
		}
		List<String> ips = new ArrayList<>();
		for (Map recordMap : records) {
			Map _sourceMap = (Map) recordMap.get("_source");
			Object ip_str = _sourceMap.get("ip_str");
			if (ip_str != null) {
				ips.add(ip_str.toString());
				//log.info(_sourceMap);
			}
		}
		return ips;
	}
	
	/**
	 * 使用rest接口获取es数据
	 */
	@SuppressWarnings({"rawtypes" })
	public List<String> getIps(String fieldName, String fieldValue,int size,String filePath) {
		String result = HttpClientUtil.get(ELASTICSEARCH_WSCAN_URL + "/_search?scroll=5m&search_type=scan&size=" + size
				+ "&q=" + fieldName + ":" + fieldValue);
		Map scrollResultMap = JSON.parseObject(result, Map.class);
		log.info(scrollResultMap);
		String _scroll_id = scrollResultMap.get("_scroll_id").toString();
		Map scrollHitMap = (Map) scrollResultMap.get("hits");
		log.info("total record:" + scrollHitMap.get("total"));

		List<String> totalIps=new ArrayList<String>();
		BufferedWriter bufferedWriter = null;
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(filePath));
			while (true) {
				result = HttpClientUtil
						.get(ELASTICSEARCH_BASE_URL + "/_search/scroll?scroll=5m&scroll_id=" + _scroll_id);
				Map resultMap = JSON.parseObject(result, Map.class);
				_scroll_id = resultMap.get("_scroll_id").toString();
				List<String> ips = parseResult(resultMap);
				if (ips == null) {
					break;
				}
				for(String ip:ips){
					bufferedWriter.write(ip);
					bufferedWriter.newLine();
				}
				bufferedWriter.flush();
				totalIps.addAll(ips);
			}
		} catch (Exception e) {
			log.error(e);
		} finally {
			try {
				bufferedWriter.flush();
				bufferedWriter.close();
			} catch (Exception e) {
			}
			// 关闭 scroll
			HttpClientUtil.delete(ELASTICSEARCH_BASE_URL + "/_search/scroll?scroll_id=" + _scroll_id);
		}
		return totalIps;
	}

	private String host;
	private String indexName;
	private String typeName;

	public EsDataService(String host, String indexName, String typeName) {
		super();
		this.host = host;
		this.indexName = indexName;
		this.typeName = typeName;
		ELASTICSEARCH_BASE_URL=host;
		ELASTICSEARCH_WSCAN_URL=host+"/"+indexName+"/"+typeName;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
	
}
