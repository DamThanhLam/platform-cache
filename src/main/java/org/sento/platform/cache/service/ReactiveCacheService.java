package org.sento.platform.cache.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collection;

@Service
@RequiredArgsConstructor
public class ReactiveCacheService {

    private final @Qualifier("jsonReactiveRedisTemplate")
    ReactiveRedisTemplate<String, Object> redisTemplate;

    private final ObjectMapper objectMapper;

    // ===================== COUNTER =====================

    public Mono<Long> increment(String key) {
        return incrementBy(key, 1);
    }

    public Mono<Long> decrement(String key) {
        return incrementBy(key, -1);
    }

    public Mono<Long> incrementBy(String key, long delta) {
        validateKey(key);

        return redisTemplate.opsForValue()
            .increment(key, delta)
            .onErrorMap(ex -> new IllegalStateException(
                "Failed to increment Redis key: " + key, ex
            ));
    }

    public Mono<Long> incrementWithTtl(String key, long delta, Duration ttl) {
        validateKey(key);
        validateTtl(ttl);

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

    public Mono<Long> decrementSafe(String key, long delta, Duration ttl) {
        validateKey(key);
        validateTtl(ttl);

        return redisTemplate.opsForValue()
            .increment(key, -delta)
            .flatMap(value -> {
                Mono<Long> normalized = value < 0
                    ? redisTemplate.opsForValue().set(key, 0L).thenReturn(0L)
                    : Mono.just(value);

                return normalized.flatMap(finalValue ->
                    redisTemplate.getExpire(key)
                        .flatMap(expire -> {
                            if (expire == null || expire.isZero() || expire.isNegative()) {
                                return redisTemplate.expire(key, ttl).thenReturn(finalValue);
                            }
                            return Mono.just(finalValue);
                        })
                );
            });
    }

    public Mono<Long> incrementBatch(String key, long totalDelta) {
        return incrementBy(key, totalDelta);
    }

    // ===================== VALUE =====================

    public <T> Mono<T> get(String key, Class<T> clazz) {
        validateKey(key);
        return redisTemplate.opsForValue()
            .get(key)
            .flatMap(value -> Mono.fromCallable(() -> objectMapper.convertValue(value, clazz)));
    }

    public <T> Mono<T> get(String key, TypeReference<T> typeReference) {
        validateKey(key);
        return redisTemplate.opsForValue()
            .get(key)
            .flatMap(value -> Mono.fromCallable(() -> objectMapper.convertValue(value, typeReference)));
    }

    public Mono<Boolean> set(String key, Object value, Duration ttl) {
        validateKey(key);
        validateTtl(ttl);

        return redisTemplate.opsForValue()
            .set(key, value, ttl);
    }

    public Mono<Boolean> setIfAbsent(String key, Object value, Duration ttl) {
        validateKey(key);
        validateTtl(ttl);

        return redisTemplate.opsForValue()
            .setIfAbsent(key, value, ttl);
    }

    public Mono<Boolean> expire(String key, Duration ttl) {
        validateKey(key);
        validateTtl(ttl);

        return redisTemplate.expire(key, ttl);
    }

    public Mono<Long> append(String key, String value) {
        validateKey(key);

        return redisTemplate.opsForValue()
            .append(key, value);
    }

    // ===================== LIST =====================

    public Mono<Long> listPush(String key, Object value) {
        validateKey(key);

        return redisTemplate.opsForList()
            .rightPush(key, value);
    }

    public Mono<Long> listPushAll(String key, Collection<?> values) {
        validateKey(key);

        return redisTemplate.opsForList()
            .rightPushAll(key, values);
    }

    public <T> Mono<T> listPop(String key, Class<T> clazz) {
        validateKey(key);

        return redisTemplate.opsForList()
            .leftPop(key)
            .flatMap(value -> Mono.fromCallable(() -> objectMapper.convertValue(value, clazz)));
    }

    public Mono<Long> listSize(String key) {
        validateKey(key);

        return redisTemplate.opsForList()
            .size(key);
    }

    // ===================== SET =====================

    public Mono<Long> setAdd(String key, Object value) {
        validateKey(key);

        return redisTemplate.opsForSet()
            .add(key, value);
    }

    public Mono<Long> setAddAll(String key, Collection<?> values) {
        validateKey(key);

        return Flux.fromIterable(values)
            .flatMap(value -> redisTemplate.opsForSet().add(key, value))
            .reduce(0L, Long::sum);
    }

    public Mono<Boolean> setIsMember(String key, Object value) {
        validateKey(key);

        return redisTemplate.opsForSet()
            .isMember(key, value);
    }

    public Mono<Long> setRemove(String key, Object value) {
        validateKey(key);

        return redisTemplate.opsForSet()
            .remove(key, value);
    }

    // ===================== COMMON =====================

    public Mono<Boolean> delete(String key) {
        validateKey(key);

        return redisTemplate.delete(key)
            .map(count -> count > 0);
    }

    public Mono<Boolean> exists(String key) {
        validateKey(key);

        return redisTemplate.hasKey(key);
    }

    public Mono<Long> ttl(String key) {
        validateKey(key);

        return redisTemplate.getExpire(key)
            .map(Duration::getSeconds);
    }

    private void validateKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Redis key must not be null or blank");
        }
    }

    private void validateTtl(Duration ttl) {
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("TTL must be greater than zero");
        }
    }
}