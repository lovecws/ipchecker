package com.surfilter.ipchecker.event.es;

import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.surfilter.ipchecker.common.es.EsClient;
import com.surfilter.ipchecker.common.es.EsPool;
import com.surfilter.ipchecker.entity.EventRecordEntity;

public abstract class BaseEvent {

	public static final String CLUSTER_NAME="elasticsearch_wa";
	public static final String HOSTNAME="127.0.0.1";
	public static final int PORT=9310;
	
	public static final Logger log=Logger.getLogger(BaseEvent.class);
	
	public abstract EventRecordEntity check(String ip);
	public static final EsPool pool=new EsPool(CLUSTER_NAME, HOSTNAME, PORT, 10);
	
	/**
	 * 事件匹配的记录
	 * 
	 * @param indexName 索引名称
	 * @param typeName 类型名称
	 * @param fieldName 匹配的字段名称
	 * @param domainIp 域名或者ip
	 * @param sortField 排序字段
	 * @return
	 */
	public EventRecordEntity record(String indexName, String typeName, String fieldName, String domainIp,
			String sortField) {
		if(domainIp==null||"".equals(domainIp)){
			return null;
		}
		EsClient esClient = pool.getEsClient();
		try {
			TransportClient transportClient = esClient.client();
			// 查询条件
			TermQueryBuilder termQueryBuilder = new TermQueryBuilder(fieldName, domainIp);
			// 记录匹配的数量
			CountResponse countResponse = transportClient.prepareCount(indexName)
					.setTypes(typeName)
					.setQuery(termQueryBuilder)
					.get();
			long count = countResponse.getCount();
			log.info(typeName+"匹配 "+domainIp + ",数量:" + count);
			if (count > 0) {
				EventRecordEntity eventRecord = new EventRecordEntity();
				eventRecord.setDomainIp(domainIp);
				eventRecord.setCount(count);

				// 获取最新的一条记录
				SearchResponse searchResponse = transportClient.prepareSearch(indexName)
						.setTypes(typeName)
						.setQuery(termQueryBuilder)
						.setFrom(0)
						.setSize(1)
						.addSort(sortField, SortOrder.DESC)
						.get();
				Map<String, Object> latestRecord = searchResponse.getHits().getAt(0).getSource();
				log.info(typeName+"匹配 "+domainIp + " 最近的一次记录:" + latestRecord);
				eventRecord.setLatestRecord(latestRecord);
				return eventRecord;
			}
		} finally {
			pool.removeEsClient(esClient);
		}

		return null;
	}
}
