package com.surfilter.ipchecker.entity;

import java.util.Map;

public class EventRecordEntity {

	private String domainIp;
	private String eventType;
	private String eventDesc;

	private long count;
	private Map<String, Object> latestRecord;

	public String getDomainIp() {
		return domainIp;
	}

	public void setDomainIp(String domainIp) {
		this.domainIp = domainIp;
	}

	public String getEventDesc() {
		return eventDesc;
	}

	public void setEventDesc(String eventDesc) {
		this.eventDesc = eventDesc;
	}

	public String getEventType() {
		return eventType;
	}

	public void setEventType(String eventType) {
		this.eventType = eventType;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public Map<String, Object> getLatestRecord() {
		return latestRecord;
	}

	public void setLatestRecord(Map<String, Object> latestRecord) {
		this.latestRecord = latestRecord;
	}

	@Override
	public String toString() {
		return "EventRecordEntity [domainIp=" + domainIp + ", eventType=" + eventType + ", eventDesc=" + eventDesc
				+ ", count=" + count + ", latestRecord=" + latestRecord + "]";
	}
	
	
}
