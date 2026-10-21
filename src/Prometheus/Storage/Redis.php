<?php

declare(strict_types=1);

namespace Prometheus\Storage;

use Prometheus\Exception\StorageException;
use Prometheus\Storage\RedisClients\PHPRedis;

class Redis extends AbstractRedis
{
    /**
     * @var mixed[]
     */
    private static $defaultOptions = [
        'host' => '127.0.0.1',
        'port' => 6379,
        'timeout' => 0.1,
        'read_timeout' => '10',
        'persistent_connections' => false,
        'password' => null,
        'user' => null,
        'sentinel' => [ // sentinel options
            'enable' => false,   // if enabled uses sentinel to get the master before connecting to redis
            'host' => '127.0.0.1',  //  phpredis sentinel address of the redis, default is the same as redis host if empty
            'port' => 26379, //  phpredis sentinel port of the primary redis server, default 26379 if empty.
            'service' => 'myprimary', //, phpredis sentinel primary name, default myprimary
            'timeout' => 0, // phpredis sentinel connection timeout
            'persistent' => null, // phpredis sentinel persistence parameter
            'retry_interval' => 0, // phpredis sentinel retry interval
            'read_timeout' => 0,  // phpredis sentinel read timeout
            'reconnect' => 0, // retries after losing connection to redis asking for a new primary, if -1 will retry indefinetely
            'username' => '', // phpredis sentinel auth username
            'password' => '', // phpredis sentinel auth password
            'ssl' => null,
        ],
    ];

    /**
     * @var mixed[]
     */
    private $options = [];

    /**
     * @var RedisSentinel|null
     */
    private $sentinel = null;

    /**
     * Redis constructor.
     *
     * @param  mixed[]  $options
     */
    public function __construct(array $options = [])
    {
        $this->options = [...self::$defaultOptions, ...$options];
        $this->options['sentinel'] = [...self::$defaultOptions['sentinel'] ?? [], ...$options['sentinel'] ?? []];
        if (boolval($this->options['sentinel']['enable'])) {
            $sentinelOptions = $options['sentinel'] ?? [];
            $sentinelOptions['host'] = $sentinelOptions['host'] ?? $this->options['host'];
            $this->sentinel = new RedisSentinel($sentinelOptions);
        }
        $this->redis = PHPRedis::create($this->options, $this->sentinel);
    }

    /**
     * Sentinels  discoverMaster
     * @return void
     */
    public function updateSentinelPrimary(): void
    {
        if ($this->redis instanceof PHPRedis) {
            $this->redis->updateSentinelPrimary();
        }
    }

    /**
     * @return \RedisSentinel
     */
    public function getRedisSentinel(): \RedisSentinel
    {
        if ($this->sentinel === null) {
            throw new StorageException('Redis Sentinel is not enabled');
        }

        return $this->sentinel->getSentinel();
    }

    /**
     * @throws StorageException
     */
    public static function fromExistingConnection(\Redis $redis, ?\RedisSentinel $redisSentinel = null): self
    {
        if (isset($redisSentinel)) {
            RedisSentinel::fromExistingConnection($redisSentinel);
        }

        if ($redis->isConnected() === false) {
            throw new StorageException('Connection to Redis server not established');
        }

        $self = new self();
        $self->redis = PHPRedis::fromExistingConnection($redis, self::$defaultOptions);

        return $self;
    }

    /**
     * @param  mixed[]  $options
     */
    public static function setDefaultOptions(array $options): void
    {
        self::$defaultOptions = array_merge(self::$defaultOptions, $options);
    }
}
