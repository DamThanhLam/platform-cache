package org.sento.platform.cache.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
@RequiredArgsConstructor
public class ReactiveCacheService {

    private final ReactiveRedisTemplate<String, Object> redisTemplate;

    public <T> Mono<T> get(String key, Class<T> clazz) {
        return redisTemplate.opsForValue()
                .get(key)
                .cast(clazz);
    }

    public Mono<Boolean> set(String key, Object value, Duration ttl) {
        return redisTemplate.opsForValue()
                .set(key, value, ttl);
    }

    public Mono<Boolean> delete(String key) {
        return redisTemplate.delete(key)
                .map(count -> count > 0);
    }
}