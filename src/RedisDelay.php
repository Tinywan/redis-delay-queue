<?php

/**
 * @desc RedisDelay
 * @date 2021/9/02 19:37
 * @author Tinywan(ShaoBo Wan)
 */

declare(strict_types=1);

namespace tinywan\redis;

class RedisDelay  extends BaseRedis
{
    const DELAY_QUEUE_PRODUCER_SCRIPT_SHA = 'DELAY:QUEUE:PRODUCER:SCRIPT:SHA';
    const DELAY_QUEUE_CONSUMER_SCRIPT_SHA = 'DELAY:QUEUE:CONSUMER:SCRIPT:SHA';

    public const  EVENT_ORDER_CLOSE = 'event:order:close';
    public const  EVENT_ORDER_CLOSE_HASH = 'event:order:close:hash';

    /**
     * @param string $keys1
     * @param string $keys2
     * @param string $member
     * @param int $score
     * @param array $message
     * @return mixed
     */
    public function producer(string $keys1, string $keys2, string $member, int $score, array $message)
    {
        $scriptSha = $this->redis->get(self::DELAY_QUEUE_PRODUCER_SCRIPT_SHA);
        if (!$scriptSha) {
            $script = <<<luascript
            redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
            redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
            return 1
luascript;
            $scriptSha = $this->redis->script('load', $script);
            $this->redis->set(self::DELAY_QUEUE_PRODUCER_SCRIPT_SHA, $scriptSha);
        }
        $hashValue = json_encode($message, JSON_UNESCAPED_UNICODE);
        return $this->redis->evalSha($scriptSha, [$keys1, $keys2, $score, $member, $hashValue], 2);
    }

    /**
     * @param string $keys1
     * @param string $keys2
     * @param int $maxScore
     * @return mixed
     */
    public function consumer(string $keys1, string $keys2, int $maxScore)
    {
        $scriptSha = $this->redis->get(self::DELAY_QUEUE_CONSUMER_SCRIPT_SHA);
        if (!$scriptSha) {
            $script = <<<luascript
            local status, type = next(redis.call('TYPE', KEYS[1]))
            if status ~= nil and status == 'ok' then
                if type == 'zset' then
                    local list = redis.call('ZREVRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'LIMIT', ARGV[3], ARGV[4])
                    if list ~= nil and #list > 0 then
                        redis.call('ZREM', KEYS[1], unpack(list))
                        local result = redis.call('HMGET', KEYS[2], unpack(list))
                        redis.call('HDEL', KEYS[2], unpack(list))
                        return result
                    end
                end
            end
            return nil
luascript;
            $scriptSha = $this->redis->script('load', $script);
            $this->redis->set(self::DELAY_QUEUE_CONSUMER_SCRIPT_SHA, $scriptSha);
        }
        return $this->redis->evalSha($scriptSha, [$keys1, $keys2, $maxScore, 0,  0, 10], 2);
    }
}
