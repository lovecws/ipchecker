package com.surfilter.ipchecker.common.es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class EsClient {

	private String clusterName;
	private String host;
	private int port;
	private TransportClient client;
	
	public EsClient(String clusterName, String host, int port) {
		super();
		this.clusterName = clusterName;
		this.host = host;
		this.port = port;
		client=client();
	}

	/**
	 * 获取es连接
	 * @param clusterName elasticsearch_wa
	 * @param hostname es主机名称
	 * @param port es端口号
	 * @return
	 */
	public TransportClient client() {
		if(client==null){
			Settings settings = ImmutableSettings.settingsBuilder()
					.put("cluster.name", clusterName)
			        .put("number_of_shards", 3)
	                .put("number_of_replicas", 0)
			        .build();
			client =  new TransportClient(settings);
			((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(host, port));
		}
		return client;
	}
	
	/**
	 * 关闭es连接
	 * @param client
	 */
	public void close(){
		if(client!=null){
			client.close();
		}
	}
}
