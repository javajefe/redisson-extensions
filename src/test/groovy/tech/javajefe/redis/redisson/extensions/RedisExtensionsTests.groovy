package tech.javajefe.redis.redisson.extensions

import org.redisson.Redisson
import org.redisson.api.RStream
import org.redisson.api.RedissonClient
import org.redisson.client.codec.StringCodec
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
    def streamName = 'TEST:REDISSON:STREAM'
    RStream stream

    def setup() {
        // Instantiate the client
        Config config = new Config()
        config.useSingleServer()
                .setAddress("redis://" + redis.getContainerIpAddress() + ":" + redis.getMappedPort(6379))
                .setDatabase(2)
        redissonClient = Redisson.create(config)
        redisExtensions = new RedisExtensions(redissonClient)
        // To create stream we have to add something into it
        stream = redissonClient.getStream(streamName, StringCodec.INSTANCE)
        // But we want to keep it Clear
        stream.remove(stream.add('dummy', 'dummy'))
    }

    def cleanup() {
        redissonClient.getKeys().flushdb()
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
            redisExtensions.batchXADD(streamName, [])
        then:
            IllegalArgumentException e = thrown()
    }

    def "Batch XADD for 1000 messages"() {
        setup:
            def streamSize = redissonClient.getStream(streamName).size()
        when:
            def ids = redisExtensions.batchXADD(streamName, (1..1000).collect {[i: it as String]})
            streamSize = redissonClient.getStream(streamName).size()
        then:
            streamSize == old(streamSize) + 1000
            ids as Set == stream.range(ids.first(), ids.last()).keySet()
    }

    def "Batch XADD does not accept null values"() {
        when:
            redisExtensions.batchXADD(streamName, [[k: null]])
        then:
            IllegalArgumentException e = thrown()
    }

    def "Batch XADD checks all messages before execution"() {
        when:
            def streamSize = stream.size()
            redisExtensions.batchXADD(streamName, [[k: 'v'], [k: null]])
        then:
            IllegalArgumentException e = thrown()
            stream.size() == streamSize

    }

    def "Batch XADD should process UTF-8 symbols correctly"() {
        setup:
            def readGroup = 'my-def-group'
            def nonASCII = 'Простой тест'
        when:
            stream.createGroup(readGroup)
            stream.add('s', nonASCII)
            def range = stream.readGroup(readGroup, 'consumer-name', 1)
        then:
            range
            range.size() == 1
            range.values().any {it.s == nonASCII}
    }

    def "XINFO Group returns information about reading group"() {
        setup:
            def readGroup = 'my-def-group'
        when:
            stream.createGroup(readGroup)
            10.times { stream.add('i', it as String) }
            def range = stream.readGroup(readGroup, 'consumer-name', 7)
            def info = redisExtensions.XINFO_GROUPS(streamName, readGroup)
        then:
            range
            info
            info.name == readGroup
            info['last-delivered-id'] == range.keySet().last() as String
    }

    def "XINFO Group throws exception for non existing reading group"() {
        when:
            redisExtensions.XINFO_GROUPS(streamName, 'unknown')
        then:
            IllegalArgumentException e = thrown()
    }

    def "getLastDeliveredId returns last read id for the reading group"() {
        setup:
            def readGroup = 'my-def-group'
            def consumerName = 'consumer-name'
        when:
            stream.createGroup(readGroup)
            10.times { stream.add('i', it as String) }
            def range = stream.readGroup(readGroup, consumerName, 7)
            def lastDeliveredId = redisExtensions.getStreamLastDeliveredId(streamName, readGroup)
        then:
            lastDeliveredId == range.keySet().last()
    }

    def "getLastDeliveredId throws exception for non existing reading group"() {
        when:
            redisExtensions.getStreamLastDeliveredId(streamName, 'unknown')
        then:
            IllegalArgumentException e = thrown()
    }

    def "getStreamTailSize returns number of messages never delivered to the reading group"() {
        setup:
            def readGroup = 'my-def-group'
            def numberOfMessages = 10
            def numberOfReads = 7
        when:
            stream.createGroup(readGroup)
            numberOfMessages.times { stream.add('i', it as String) }
            stream.readGroup(readGroup, 'consumer-name', numberOfReads)
            def tailSize = redisExtensions.getStreamTailSize(streamName, readGroup)
        then:
            tailSize == numberOfMessages - numberOfReads
    }
}
