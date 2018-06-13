package com.surfilter.ipchecker.event;

import com.surfilter.ipchecker.entity.EventRecordEntity;

/**
 * 网站后门攻击 根据域名进行匹配
 * @author ganliang
 *
 */
public class WebBackDoorEvent extends BaseEvent{
	public static final String INDEX_NAME="web_back_door_index";
	public static final String INDEX_TYPE="web_back_door";
	public static final String IP_POINT_FIELD="dst_ip_point"; //domain
	public static final String SORT_FIELD="submit_time";
	
	public EventRecordEntity check(String ip){
		EventRecordEntity record = record(INDEX_NAME,INDEX_TYPE,IP_POINT_FIELD,ip,SORT_FIELD);
		if(record!=null){
			record.setEventType("web_back_door");
			record.setEventDesc("网站后门攻击");
		}
		return record;
	}
}
