package com.netty.zookeeper.listener;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
@Slf4j
@Getter
@Setter
public class ZookeeperPathListenter implements PathChildrenCacheListener {
	private PathChildrenCache pathChildrenCache;
	@Override
	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
		switch (event.getType()) {
		case CHILD_ADDED:
			log.info("add server node {}",new String(event.getData().getData(),"utf-8"));
			break;
		case CHILD_UPDATED:
			log.info("update server node {}",new String(event.getData().getData(),"utf-8"));
			break;
		case CHILD_REMOVED:
			log.info("remove server node {}",new String(event.getData().getData(),"utf-8"));
			break;
		default:
			break;
		}
	}

}
