package com.surfilter.ipchecker.common.es;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;

public class EsPoolableFactory extends BasePoolableObjectFactory<EsClient> {

	public static final Logger log = Logger.getLogger(EsPoolableFactory.class);
	private String clusterName;
	private String host;
	private int port;

	public EsPoolableFactory(String clusterName, String host, int port) {
		super();
		this.clusterName = clusterName;
		this.host = host;
		this.port = port;
	}

	@Override
	public EsClient makeObject() throws Exception {
		EsClient esClient = new EsClient(clusterName, host, port);
		log.info("create EsClient! " + esClient);
		System.out.println("create EsClient! " + esClient);
		return esClient;
	}

	@Override
	public void destroyObject(EsClient esClient) throws Exception {
		esClient.close();
		log.info("destroyObject EsClient! " + esClient);
		System.out.println("destroyObject EsClient! " + esClient);
		super.destroyObject(esClient);
	}

	@Override
	public void passivateObject(EsClient obj) throws Exception {
		log.info("return to pool");
		super.passivateObject(obj);
	}

}
