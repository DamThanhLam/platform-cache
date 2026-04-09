package org.sento.platform.cache.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

@Service
public class ReactiveCacheService {

    @Qualifier("jsonReactiveRedisTemplate")
    private ReactiveRedisTemplate<String, Object> redisTemplate;

    // ===================== VALUE =====================

    public <T> Mono<T> get(String key, Class<T> clazz) {
        return redisTemplate.opsForValue()
            .get(key)
            .cast(clazz);
    }

    /**
     * Overwrite value + reset TTL
     */
    public Mono<Boolean> set(String key, Object value, Duration ttl) {
        return redisTemplate.opsForValue()
            .set(key, value, ttl);
    }

    /**
     * Only set if key does NOT exist (SETNX)
     */
    public Mono<Boolean> setIfAbsent(String key, Object value, Duration ttl) {
        return redisTemplate.opsForValue()
            .setIfAbsent(key, value, ttl);
    }

    /**
     * Update TTL only (keep value)
     */
    public Mono<Boolean> expire(String key, Duration ttl) {
        return redisTemplate.expire(key, ttl);
    }

    /**
     * Append string value (only for String serialization)
     */
    public Mono<Long> append(String key, String value) {
        return redisTemplate.opsForValue()
            .append(key, value);
    }

    // ===================== LIST =====================

    public Mono<Long> listPush(String key, Object value) {
        return redisTemplate.opsForList()
            .rightPush(key, value);
    }

    public Mono<Long> listPushAll(String key, Collection<?> values) {
        return redisTemplate.opsForList()
            .rightPushAll(key, values);
    }

    public <T> Mono<T> listPop(String key, Class<T> clazz) {
        return redisTemplate.opsForList()
            .leftPop(key)
            .cast(clazz);
    }

    public Mono<Long> listSize(String key) {
        return redisTemplate.opsForList()
            .size(key);
    }

    // ===================== SET =====================

    /**
     * Add (no duplicate) → best for JWT jti
     */
    public Mono<Long> setAdd(String key, Object value) {
        return redisTemplate.opsForSet()
            .add(key, value);
    }

    public Mono<Boolean> setIsMember(String key, Object value) {
        return redisTemplate.opsForSet()
            .isMember(key, value);
    }

    public Mono<Long> setRemove(String key, Object value) {
        return redisTemplate.opsForSet()
            .remove(key, value);
    }

    // ===================== COMMON =====================

    public Mono<Boolean> delete(String key) {
        return redisTemplate.delete(key)
            .map(count -> count > 0);
    }

    public Mono<Boolean> exists(String key) {
        return redisTemplate.hasKey(key);
    }

    public Mono<Long> ttl(String key) {
        return redisTemplate.getExpire(key)
            .map(Duration::getSeconds);
    }
}