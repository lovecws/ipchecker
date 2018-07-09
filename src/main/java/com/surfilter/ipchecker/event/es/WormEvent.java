package com.surfilter.ipchecker.event.es;

import com.surfilter.ipchecker.entity.EventRecordEntity;

/**
 * 蠕虫
 * @author ganliang
 *
 */
public class WormEvent extends BaseEvent{

	public static final String INDEX_NAME="worm_index";
	public static final String INDEX_TYPE="worm";
	public static final String IP_POINT_FIELD="ip_point";
	public static final String SORT_FIELD="submit_time";
	
	public EventRecordEntity check(String ip){
		EventRecordEntity record = record(INDEX_NAME,INDEX_TYPE,IP_POINT_FIELD,ip,SORT_FIELD);
		if(record!=null){
			record.setEventType("worm");
			record.setEventDesc("蠕虫病毒");
		}
		return record;
	}
	
}
