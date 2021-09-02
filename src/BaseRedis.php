<?php

/**
 * @desc BaseRedis
 * @date 2021/9/02 19:37
 * @author Tinywan(ShaoBo Wan)
 */

declare(strict_types=1);

namespace tinywan\redis;

class BaseRedis
{
    /**
     * @var \Redis
     */
    protected $redis;

    public function __construct(array $config)
    {
        if (!\class_exists('redis', false)) {
            throw new \Exception('not support: Redis，必须安装redis扩展，请检查扩展是否安装');
        }
        $this->redis = new \Redis();
        $this->redis->connect($config['host'],$config['port']);
        if($config['auth']){
            $this->redis->auth($config['auth']);
        }
    }
}
