package org.sento.platform.cache.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
@RequiredArgsConstructor
public class DistributedLockService {

    private final ReactiveRedisTemplate<String, Object> redisTemplate;

    public Mono<Boolean> tryLock(String key, String value, Duration ttl) {
        return redisTemplate.opsForValue()
                .setIfAbsent(key, value, ttl);
    }

    public Mono<Boolean> unlock(String key) {
        return redisTemplate.delete(key)
                .map(count -> count > 0);
    }
}
