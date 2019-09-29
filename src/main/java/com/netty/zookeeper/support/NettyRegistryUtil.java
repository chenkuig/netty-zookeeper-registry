package com.netty.zookeeper.support;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.ApplicationObjectSupport;
import org.springframework.util.Assert;

import com.netty.zookeeper.registry.config.CoordinatorRegistryCenter;

import lombok.Getter;
@Configuration
public class NettyRegistryUtil extends ApplicationObjectSupport implements SmartInitializingSingleton{
	@Getter
	private static CoordinatorRegistryCenter ZOOKEEPER_CENTER;
	
	public static void registryServer(String key, String value) {
		if (StringUtils.isBlank(key)) {
			Assert.isTrue(false, "key is not null");
		}
		if (StringUtils.isBlank(value)) {
			value = key;
		}
		if (!key.startsWith("/")) {
			key = "/"+key;
		}
		ZOOKEEPER_CENTER.persistEphemeral(key, value);
		ZOOKEEPER_CENTER.addCacheData(key);
	}
	
	public static void registryServer(String server) {
		if (StringUtils.isBlank(server)) {
			Assert.isTrue(false, "server is not null");
		}
		String key = "";
		if (!server.startsWith("/")) {
			key = "/"+server;
		}
		ZOOKEEPER_CENTER.persistEphemeral(key, server);
		ZOOKEEPER_CENTER.addCacheData(key);
	}
	@Override
	public void afterSingletonsInstantiated() {
		ApplicationContext context = getApplicationContext();
	    if (context != null) {
	    	ZOOKEEPER_CENTER  =  context.getBean(CoordinatorRegistryCenter.class);
	    }
	}
}
