package org.sento.platform.cache.service;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

@Service
public class ReactiveCacheService {

    private final ReactiveRedisTemplate<String, Object> redisTemplate;

    public ReactiveCacheService(
        @Qualifier("jsonReactiveRedisTemplate")
        ReactiveRedisTemplate<String, Object> redisTemplate
    ) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * Increment by 1 (atomic)
     */
    public Mono<Long> increment(String key) {
        return incrementBy(key, 1);
    }

    /**
     * Decrement by 1 (atomic)
     */
    public Mono<Long> decrement(String key) {
        return incrementBy(key, -1);
    }

    /**
     * Increment by delta (supports negative)
     */
    public Mono<Long> incrementBy(String key, long delta) {
        validateKey(key);

        return redisTemplate.opsForValue()
            .increment(key, delta)
            .onErrorMap(ex -> new IllegalStateException(
                "Failed to increment key=" + key + ", ensure value is numeric", ex
            ));
    }

    /**
     * Increment and ensure TTL exists (idempotent TTL set)
     */
    public Mono<Long> incrementWithTtl(String key, long delta, Duration ttl) {
        validateKey(key);

        return redisTemplate.opsForValue()
            .increment(key, delta)
            .flatMap(value ->
                redisTemplate.getExpire(key)
                    .flatMap(expire -> {
                        if (expire == null || expire.isZero() || expire.isNegative()) {
                            return redisTemplate.expire(key, ttl).thenReturn(value);
                        }
                        return Mono.just(value);
                    })
            );
    }

    /**
     * Decrement safely (never go below 0)
     */
    public Mono<Long> decrementSafe(String key, long delta, Duration ttl) {
        validateKey(key);

        return redisTemplate.opsForValue()
            .increment(key, -delta)
            .flatMap(value -> {

                Mono<Long> result;

                if (value < 0) {
                    result = redisTemplate.opsForValue()
                        .set(key, 0L)
                        .thenReturn(0L);
                } else {
                    result = Mono.just(value);
                }

                return result.flatMap(finalValue ->
                    redisTemplate.getExpire(key)
                        .flatMap(expire -> {
                            if (expire == null || expire.isZero() || expire.isNegative()) {
                                return redisTemplate.expire(key, ttl)
                                    .thenReturn(finalValue);
                            }
                            return Mono.just(finalValue);
                        })
                );
            });
    }

    /**
     * Batch increment (optimized for consumer)
     */
    public Mono<Long> incrementBatch(String key, long totalDelta) {
        return incrementBy(key, totalDelta);
    }

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
        return this.redisTemplate.opsForSet().add(key, value);
    }

    public Mono<Long> setAddAll(String key, Collection<?> values) {
        return Flux.fromIterable(values)
            .flatMap(value -> redisTemplate.opsForSet().add(key, value))
            .reduce(0L, Long::sum);
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

    private void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Redis key must not be null or blank");
        }
    }
}