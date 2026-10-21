<?php

declare(strict_types=1);

namespace Prometheus\Storage\RedisClients;

use Prometheus\Exception\StorageException;
use Prometheus\Storage\RedisSentinel;

class PHPRedis implements RedisClient
{
    // The following array contains all exception message parts which are interpreted as a connection loss or
    // another unavailability of Redis.
    private const ERROR_MESSAGES_INDICATING_UNAVAILABILITY = [
        'connection closed',
        'connection refused',
        'connection lost',
        'failed while reconnecting',
        'is loading the dataset in memory',
        'php_network_getaddresses',
        'read error on connection',
        'socket',
        'went away',
        'loading',
        'readonly',
        "can't write against a read only replica",
    ];

    /**
     * @var \Redis
     */
    private $redis;

    /**
     * @var mixed[]
     */
    private $options = [];

    /**
     * @var RedisSentinel|null
     */
    private $sentinel = null;

    /**
     * @var bool
     */
    private $connectionInitialized = false;

    /**
     * @param  mixed[]  $options
     */
    public function __construct(\Redis $redis, array $options, ?RedisSentinel $sentinel = null)
    {
        $this->redis = $redis;
        $this->options = $options;
        $this->sentinel = $sentinel;
    }

    /**
     * @param  mixed[]  $options
     */
    public static function create(array $options, ?RedisSentinel $sentinel = null): self
    {
        $redis = new \Redis();

        return new self($redis, $options, $sentinel);
    }

    /**
     * Sentinels  discoverMaster
     * @return void
     */
    public function updateSentinelPrimary(): void
    {
        if ($this->sentinel === null) {
            return;
        }

        $master = $this->sentinel->getMaster();

        if (is_array($master)) {
            $this->options['host'] = $master['ip'];
            $this->options['port'] = $master['port'];
        }
    }

    /**
     * @param  mixed[]  $options
     */
    public static function fromExistingConnection(\Redis $redis, array $options): self
    {
        $self = new self($redis, $options);
        $self->connectionInitialized = true;

        return $self;
    }

    public function getPrefix(): ?string
    {
        /** @var mixed $prefix */
        $prefix = $this->redis->getOption(\Redis::OPT_PREFIX);

        return is_string($prefix) && $prefix !== '' ? $prefix : null;
    }

    public function eval(string $script, array $args = [], int $num_keys = 0): void
    {
        $this->redis->eval($script, $args, $num_keys);
    }

    public function set(string $key, mixed $value, mixed $options = null): bool
    {
        return $this->redis->set($key, $value, $options);
    }

    public function setNx(string $key, mixed $value): void
    {
        $this->redis->setNx($key, $value); /** @phpstan-ignore-line */
    }

    public function sMembers(string $key): array
    {
        return $this->redis->sMembers($key);
    }

    public function hGetAll(string $key): array|false
    {
        return $this->redis->hGetAll($key);
    }

    public function keys(string $pattern): array
    {
        return $this->redis->keys($pattern);
    }

    public function get(string $key): string|false
    {
        return $this->redis->get($key);
    }

    /**
     * @throws RedisClientException
     */
    public function del(array|string $key, string ...$other_keys): void
    {
        try {
            $this->redis->del($key, ...$other_keys);
        } catch (\RedisException $e) {
            throw new RedisClientException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws StorageException
     */
    public function ensureOpenConnection(): void
    {
        if ($this->connectionInitialized === true) {
            return;
        }

        if ($this->sentinel !== null) {
            $this->connectThroughSentinel();
        } else {
            $this->connectToServer();
        }

        $authParams = [];

        if (isset($this->options['user']) && $this->options['user'] !== '') {
            $authParams[] = $this->options['user'];
        }

        if (isset($this->options['password'])) {
            $authParams[] = $this->options['password'];
        }

        if ($authParams !== []) {
            $this->redis->auth($authParams);
        }

        if (isset($this->options['database'])) {
            $this->redis->select($this->options['database']);
        }

        $this->redis->setOption(\Redis::OPT_READ_TIMEOUT, $this->options['read_timeout']);

        $this->connectionInitialized = true;
    }

    /**
     * Asks Sentinel for the current primary and connects to it. Retries up to `sentinel.reconnect` times
     * (indefinitely if -1) while the error indicates that Redis is unavailable or readonly, which may happen
     * during a Sentinel failover; any other error, or running out of retries, is thrown.
     *
     * @throws StorageException
     */
    private function connectThroughSentinel(): void
    {
        $reconnect = (int) ($this->options['sentinel']['reconnect'] ?? 0);
        $retries = 0;
        while (true) {
            try {
                $this->updateSentinelPrimary();
                $this->connectToServer();

                return;
            } catch (StorageException $e) {
                $cause = $e->getPrevious();
                $retryable = $cause instanceof \RedisException && $this->isRedisUnavailableOrReadonly($cause);
                if (! $retryable || ($reconnect >= 0 && $retries >= $reconnect)) {
                    throw $e;
                }
            }
            $retries++;
        }
    }

    /**
     * Inspects the given exception and tells whether the reported error indicates that the server
     * went away or is in readonly mode, which may happen in case of a Redis Sentinel failover.
     */
    private function isRedisUnavailableOrReadonly(\RedisException $exception): bool
    {
        // We convert the exception message to lower-case in order to perform case-insensitive comparison.
        $exceptionMessage = strtolower($exception->getMessage());

        // Because we also match only partial exception messages, we cannot use in_array() at this point.
        foreach (self::ERROR_MESSAGES_INDICATING_UNAVAILABILITY as $errorMessage) {
            if (str_contains($exceptionMessage, $errorMessage)) {
                return true;
            }
        }

        return false;
    }

    /**
     * @throws StorageException
     */
    private function connectToServer(): void
    {
        try {
            $connection_successful = false;
            if ($this->options['persistent_connections'] !== false) {
                $connection_successful = $this->redis->pconnect(
                    $this->options['host'],
                    (int) $this->options['port'],
                    (float) $this->options['timeout']
                );
            } else {
                $connection_successful = $this->redis->connect($this->options['host'], (int) $this->options['port'], (float) $this->options['timeout']);
            }
            if (! $connection_successful) {
                throw new StorageException(
                    sprintf("Can't connect to Redis server. %s", $this->redis->getLastError()),
                    0
                );
            }
        } catch (\RedisException $e) {
            throw new StorageException(
                sprintf("Can't connect to Redis server. %s", $e->getMessage()),
                $e->getCode(),
                $e,
            );
        }
    }
}
