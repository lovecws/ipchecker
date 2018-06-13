package com.surfilter.ipchecker.util;

import java.util.ArrayList;
import java.util.List;

import com.surfilter.ipchecker.entity.EventRecordEntity;
import com.surfilter.ipchecker.event.BaseEvent;
import com.surfilter.ipchecker.event.BotOrTrojanControllEvent;
import com.surfilter.ipchecker.event.BotOrTrojanEvent;
import com.surfilter.ipchecker.event.MalwareHostingSiteEvent;
import com.surfilter.ipchecker.event.WebBackDoorEvent;
import com.surfilter.ipchecker.event.WebDefacementEvent;
import com.surfilter.ipchecker.event.WormEvent;

public class EventUtil {

	public static BaseEvent botOrTrojanControll = new BotOrTrojanControllEvent();
	public static BaseEvent botOrTrojan = new BotOrTrojanEvent();
	public static BaseEvent malwareHostingSite = new MalwareHostingSiteEvent();
	public static BaseEvent webBackDoor = new WebBackDoorEvent();
	public static BaseEvent webDefacement = new WebDefacementEvent();
	public static BaseEvent worm = new WormEvent();

	/**
	 * 
	 * @param ip ip地址信息
	 * @param domain 域名信息
	 * @return
	 */
	public static List<EventRecordEntity> event(String ip,String domain) {
		List<EventRecordEntity> eventRecords = new ArrayList<EventRecordEntity>();
		// 僵木控制
		EventRecordEntity botOrTrojanControllEventRecord = botOrTrojanControll.check(ip);
		if (botOrTrojanControllEventRecord != null) {
			eventRecords.add(botOrTrojanControllEventRecord);
		}
		// 僵木受控
		EventRecordEntity botOrTrojanEventRecord = botOrTrojan.check(ip);
		if (botOrTrojanEventRecord != null) {
			eventRecords.add(botOrTrojanEventRecord);
		}
		// 网页放马
		EventRecordEntity malwareHostingSiteEventRecord = malwareHostingSite.check(domain);
		if (malwareHostingSiteEventRecord != null) {
			eventRecords.add(malwareHostingSiteEventRecord);
		}
		// 网页后门
		EventRecordEntity webBackDoorEventRecord = webBackDoor.check(domain);
		if (webBackDoorEventRecord != null) {
			eventRecords.add(webBackDoorEventRecord);
		}
		// 网页篡改
		EventRecordEntity webDefacementEventRecord = webDefacement.check(domain);
		if (webDefacementEventRecord != null) {
			eventRecords.add(webDefacementEventRecord);
		}
		// 蠕虫
		EventRecordEntity wormEventRecord = worm.check(ip);
		if (wormEventRecord != null) {
			eventRecords.add(wormEventRecord);
		}
		return eventRecords;
	}
}
