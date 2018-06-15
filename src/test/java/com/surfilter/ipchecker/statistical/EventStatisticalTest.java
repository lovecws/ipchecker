package com.surfilter.ipchecker.statistical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import com.alibaba.fastjson.JSON;

public class EventStatisticalTest {

	@Test
	public void eventType(){
		String projectDir = System.getProperty("user.dir").replace("\\", "/");
		Map<String, Integer> eventType = EventStatistical.fieldCounter("eventType",projectDir+"/event/hubei/20180612181436/event.json", null);
		
		for(Entry<String, Integer> entry:eventType.entrySet()){
			System.out.println(JSON.toJSONString(entry));
		}
	}

	@Test
	public void ipOperator(){
		String projectDir = System.getProperty("user.dir").replace("\\", "/");
		Map<String, Integer> eventType = EventStatistical.fieldCounter("latestRecord.ip_operator",projectDir+"/event/hubei/event20180611141246.json", null);
		
		for(Entry<String, Integer> entry:eventType.entrySet()){
			System.out.println(JSON.toJSONString(entry));
		}
	}

	@Test
	public void eventTypeAndipOperator(){
		String projectDir = System.getProperty("user.dir").replace("\\", "/");
		Map<String, Integer> eventType = EventStatistical.fieldCounter("eventType,latestRecord.ip_operator",projectDir+"/event/hubei/event20180611141246.json", null);
		
		for(Entry<String, Integer> entry:eventType.entrySet()){
			System.out.println(JSON.toJSONString(entry));
		}
	}

	@Test
	public void ipOperatorAndEventType(){
		String projectDir = System.getProperty("user.dir").replace("\\", "/");
		Map<String, Integer> eventType = EventStatistical.fieldCounter("latestRecord.ip_operator,eventDesc",projectDir+"/event/hubei/event20180611141246.json", null);
		
		List<Map.Entry<String,Integer>> list = new ArrayList<Map.Entry<String,Integer>>(eventType.entrySet());
		Collections.sort(list,new Comparator<Map.Entry<String,Integer>>() {
            //升序排序
            public int compare(Entry<String, Integer> o1,
                    Entry<String, Integer> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
		
		for(Entry<String, Integer> entry:eventType.entrySet()){
			System.out.println(JSON.toJSONString(entry));
		}
	}
	
	@Test
	public void statistical(){
		String projectDir = System.getProperty("user.dir").replace("\\", "/");
		EventStatistical.statistical(projectDir+"/event/event20180611141246.json", projectDir+"/event/hubei/event20180611141246_statistical.json");
	}

	@Test
	public void statisticalModel(){
		String projectDir = System.getProperty("user.dir").replace("\\", "/");
		EventStatistical.statisticalModel(projectDir+"/event/jiangxi/20180612160305/part-00000", projectDir+"/event/jiangxi/20180612160305/statistical.csv");
	}
}
