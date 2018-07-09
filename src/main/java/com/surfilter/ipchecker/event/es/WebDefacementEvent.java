package com.surfilter.ipchecker.event.es;

import com.surfilter.ipchecker.entity.EventRecordEntity;

/**
 * 网页篡改 根据域名进行匹配
 * @author ganliang
 *
 */
public class WebDefacementEvent extends BaseEvent{
	public static final String INDEX_NAME="web_defacement_index";
	public static final String INDEX_TYPE="web_defacement";
	public static final String IP_POINT_FIELD="ip_point"; //domain
	public static final String SORT_FIELD="submit_time";
	
	public EventRecordEntity check(String ip){
		EventRecordEntity record = record(INDEX_NAME,INDEX_TYPE,IP_POINT_FIELD,ip,SORT_FIELD);
		if(record!=null){
			record.setEventType("web_defacement");
			record.setEventDesc("网页篡改");
		}
		return record;
	}
}
