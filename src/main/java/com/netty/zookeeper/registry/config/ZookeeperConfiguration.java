package com.netty.zookeeper.registry.config;

import javax.annotation.Resource;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@EnableConfigurationProperties(ZookeeperProperties.class)
@Configuration
public class ZookeeperConfiguration {
	/** zk中心 */
    public static final String ZOOKEEPER_REGISTRY_CENTER_BEAN_NAME = "nettyZookeeperRegistryCenter";
	@Resource
	private ZookeeperProperties zookeeperProperties;
	
	/**
     * zookeeper 注册中心
     * @return zk注册中心实例对象
     */
    @Bean(name = ZOOKEEPER_REGISTRY_CENTER_BEAN_NAME, initMethod = "init")
    @ConditionalOnMissingBean
    public ZookeeperRegistryCenter zookeeperRegistryCenter() {
        ZookeeperRegistryCenter zookeeperRegistryCenter = new ZookeeperRegistryCenter(zookeeperProperties);
        return zookeeperRegistryCenter;
    }
}
