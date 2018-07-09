package com.surfilter.ipchecker.event.es;

import com.surfilter.ipchecker.entity.EventRecordEntity;

/**
 * 僵木受控 根据源ip匹配
 * @author ganliang
 *
 */
public class BotOrTrojanEvent extends BaseEvent{

	public static final String INDEX_NAME="bot_or_trojan_index";
	public static final String INDEX_TYPE="bot_or_trojan";
	public static final String IP_POINT_FIELD="src_ip_point";
	public static final String SORT_FIELD="submit_time";
	
	public EventRecordEntity check(String ip){
		EventRecordEntity record = record(INDEX_NAME,INDEX_TYPE,IP_POINT_FIELD,ip,SORT_FIELD);
		if(record!=null){
			record.setEventType("bot_or_trojan");
			record.setEventDesc("僵木受控");
		}
		return record;
	}
}
