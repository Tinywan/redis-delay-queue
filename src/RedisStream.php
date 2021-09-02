<?php

/**
 * @desc RedisStream
 * @date 2021/9/02 19:37
 * @author Tinywan(ShaoBo Wan)
 */

declare(strict_types=1);

namespace tinywan\redis;

class RedisStream  extends BaseRedis
{
    /**
     * stream 是否已经在group
     *
     * @param string $stream
     * @param string $group
     * @return bool
     */
    public function groupExists(string $stream, string $group): bool
    {
        $groups = $this->redis->xInfo('GROUPS', $stream);
        foreach ($groups as $_group) {
            if ($_group['name'] === $group) {
                return true;
            }
        }
        return false;
    }
    /**
     * @param string $streamKey
     * @param string $group
     * @param array $messageFields
     * @return mixed
     */
    public function producer(string $streamKey, string $group, array $messageFields)
    {
        $this->redis->xAdd($streamKey, '*', $messageFields, 1000, true);
        if (false === self::groupExists($streamKey, $group)) {
            return $this->redis->xGroup('CREATE', $streamKey, $group, '0');
        }
        return true;
    }

    /**
     * @param string $streamKey
     * @param string $group
     * @param string $consumer
     * @return void
     */
    public function consumers(string $streamKey, string $group, string $consumer)
    {
        $entries = $this->redis->xReadGroup($group, $consumer, [$streamKey => '>'], 10, 2000);
        if (empty($entries)) {
            echo ' [x] Message List is Empty, Try Again ' . $consumer, "\n";
            return;
        }
        foreach ($entries as $streamEntries) {
            if (!empty($streamEntries)) {
                foreach ($streamEntries as $messageId => $messageField) {
                    $processRes = self::processConsumer($messageId, $messageField);
                    if ($processRes === true) {
                        $ackRes = $this->redis->xAck($streamKey, $group, [$messageId]); // return 1
                        if ($ackRes === 1) {
                            echo ' [x] ACK OK', "\n";
                        } else {
                            echo ' [x] message ack fail ', "\n";
                        }
                    }
                }
            }
        }
    }

    /**
     * @param string $streamKey
     * @param string $group
     * @param array $consumers
     *
     * @return void
     */
    public function consumersPending(string $streamKey, string $group, array $consumers)
    {
        foreach ($consumers as $consumer) {
            $entries = $this->redis->xReadGroup($group, $consumer, [$streamKey => '0'], 10, 2000);
            if (empty($entries) || count($entries[$streamKey]) === 0) {
                echo ' [x] message is empty ' . $consumer, "\n";
                continue;
            }
            foreach ($entries as $streamEntries) {
                foreach ($streamEntries as $messageId => $messageField) {
                    $processRes = self::processConsumer($messageId, $messageField);
                    if ($processRes === true) {
                        $ackRes = $this->redis->xAck($streamKey, $group, [$messageId]);
                        if ($ackRes === 1) {
                            echo ' [x] ACK OK', "\n";
                        } else {
                            echo ' [x] message ack fail', "\n";
                        }
                    }
                }
            }
        }
    }

    /**
     * @param string $messageId
     * @param array $messageFields
     * @return bool
     */
    private static function processConsumer(string $messageId, array $messageFields)
    {
        return true;
    }
}
