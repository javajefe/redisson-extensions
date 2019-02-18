package org.javajefe.redis.redisson.extensions

import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.Config
import org.testcontainers.containers.GenericContainer
import org.testcontainers.spock.Testcontainers
import spock.lang.Shared
import spock.lang.Specification

/**
 * Created by BukarevAA on 18.02.2019.
 */
@Testcontainers
class RedisExtensionsTests extends Specification {

    @Shared
    GenericContainer redis = new GenericContainer("redis:5.0.3")
            .withExposedPorts(6379)
    RedissonClient redissonClient
    RedisExtensions redisExtensions

    def setup() {
        // Instantiate the client
        Config config = new Config()
        config.useSingleServer()
                .setAddress("redis://" + redis.getContainerIpAddress() + ":" + redis.getMappedPort(6379))
                .setDatabase(2)
        redissonClient = Redisson.create(config)
        redisExtensions = new RedisExtensions(redissonClient)
    }

    def cleanup() {
        redissonClient.shutdown()
    }

    def "Empty key is not allowed in Batch XADD"() {
        when:
            redisExtensions.batchXADD('', [[k: 'v']])
        then:
            IllegalArgumentException e = thrown()
    }

    def "Empty message list is not allowed in Batch XADD"() {
        when:
            redisExtensions.batchXADD('k', [])
        then:
            IllegalArgumentException e = thrown()
    }

    def "Batch XADD for 1000 messages"() {
        setup:
            def stream = 'TEST:REDISSON:XADD'
            def streamSize = redissonClient.getStream(stream).size()
        when:
            redisExtensions.batchXADD(stream, (1..1000).collect {[i: it as String]})
            streamSize = redissonClient.getStream(stream).size()
        then:
            streamSize == old(streamSize) + 1000
    }
}
