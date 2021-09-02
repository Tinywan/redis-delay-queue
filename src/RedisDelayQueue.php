<?php

/**
 * @desc RedisDelayQueue
 * @date 2021/9/02 19:37
 * @author Tinywan(ShaoBo Wan)
 */

declare(strict_types=1);

namespace tinywan\redis;

class RedisDelayQueue
{
    /**
     * @author Tinywan(ShaoBo Wan)
     */
    public function consumer()
    {
        while (true) {
            $maxScore = time();
            $queueList = RedisDelay::consumer(RedisDelay::EVENT_ORDER_CLOSE,RedisDelay::EVENT_ORDER_CLOSE_HASH, $maxScore);
            if (false === $queueList) {
                sleep(1);
                continue;
            }
            try {
                foreach ($queueList as $queue) {
                    $queueArr = json_decode($queue,true);
                    switch ($queueArr['event']) {
                        case RedisDelayQueue::EVENT_ORDER_CLOSE:
                            self::delayProcessing($queueArr);
                            break;
                        default:
                            echo ' [x] 未知消息类型';
                    }
                }
            } catch (\Exception $exception) {
                echo ' [x] 未知消息类型';
            }
        }
    }
}
