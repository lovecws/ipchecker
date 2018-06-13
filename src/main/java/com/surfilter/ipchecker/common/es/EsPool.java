package com.surfilter.ipchecker.common.es;

import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;

public class EsPool {

	protected static final Logger logger = Logger.getLogger(EsPool.class);

	private static StackObjectPool<EsClient> pool = null;

	public EsPool(String clusterName, String ip, int port, int keepClienNum) {
		if (pool == null) {
			pool = new StackObjectPool<EsClient>(new EsPoolableFactory(clusterName,ip,port), keepClienNum);
		}
	}
	
	public EsClient getEsClient(){
    	EsClient esClient = null;
    	try {
			esClient = pool.borrowObject();
		} catch (Exception e) {
			logger.error("create Client error!" , e);
		}
    	return esClient;
    }
    
    public void removeEsClient(EsClient esClient){
    	try {
			pool.returnObject(esClient);
		} catch (Exception e) {
			logger.error("Client return to pool error!" , e);
		}
    }
    
	public void destroyObject(EsClient esClient) throws Exception {
		try {
			 pool.invalidateObject(esClient);
		} catch (Exception e) {
			logger.error("Client return to pool error!" , e);
		}
	}
}
