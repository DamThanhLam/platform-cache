package org.sento.platform.cache.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

@Configuration
@ComponentScan(basePackages = "org.sento.platform.cache")
@ConditionalOnClass(RedisConnectionFactory.class)
public class CacheAutoConfiguration {
}