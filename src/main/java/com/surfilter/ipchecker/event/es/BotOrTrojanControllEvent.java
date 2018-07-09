package com.surfilter.ipchecker.event.es;

import com.surfilter.ipchecker.entity.EventRecordEntity;

/**
 * 僵木控制 根据目标ip进行匹配
 * @author ganliang
 *
 */
public class BotOrTrojanControllEvent extends BaseEvent{

	public static final String INDEX_NAME="bot_or_trojan_c_index";
	public static final String INDEX_TYPE="bot_or_trojan_c";
	public static final String IP_POINT_FIELD="dst_ip_point";
	public static final String SORT_FIELD="submit_time";
	
	public EventRecordEntity check(String ip){
		EventRecordEntity record = record(INDEX_NAME,INDEX_TYPE,IP_POINT_FIELD,ip,SORT_FIELD);
		if(record!=null){
			record.setEventType("bot_or_trojan_c");
			record.setEventDesc("僵木控制");
		}
		return record;
	}
}
