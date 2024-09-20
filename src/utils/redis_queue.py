import json
import asyncio
import os
from typing import Optional, Dict, Any
from redis import Redis
from redis.exceptions import RedisError
import logging

# Configure logging for RedisQueue operations
logger = logging.getLogger("RedisQueue")
logger.setLevel(logging.INFO)


class RedisQueue:
    """
    RedisQueue provides an optimized, production-ready interface for managing task queues.
    This class handles queuing tasks in Redis with thread-safe operations, 
    ensuring error handling, locking, and race condition prevention.
    """

    def __init__(self):
        """
        Initialize the Redis client and an asyncio lock for thread-safe operations.
        The Redis client is configured based on environment variables.
        """
        self.client = self._create_redis_client()
        self.lock = asyncio.Lock()

    def _create_redis_client(self) -> Redis:
        """
        Create and configure a Redis client based on environment variables.

        :return: Redis client instance.
        """
        return Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            password=os.getenv('REDIS_PASSWORD', None),
            decode_responses=True  # Ensures the data is returned as a string, not bytes.
        )

    async def push_task(self, queue_name: str, task: Dict[str, Any]) -> bool:
        """
        Push a task to the specified Redis queue in a thread-safe manner.
        Converts the task to JSON and pushes it to the queue.

        :param queue_name: Name of the Redis queue.
        :param task: Dictionary containing task data.
        :return: True if the task was successfully pushed, False otherwise.
        """
        async with self.lock:
            try:
                task_json = json.dumps(task)
                await asyncio.to_thread(self.client.rpush, queue_name, task_json)
                logger.info(f"Task successfully pushed to queue '{queue_name}'.")
                return True
            except (RedisError, TypeError, json.JSONDecodeError) as e:
                logger.error(f"Error pushing task to queue '{queue_name}': {e}")
                return False

    async def pop_task(self, queue_name: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
        """
        Pop a task from the Redis queue, blocking for up to `timeout` seconds.
        Uses Redis' BLPOP command to block until a task is available.

        :param queue_name: Name of the Redis queue.
        :param timeout: Timeout in seconds before returning if no task is available.
        :return: A dictionary representing the task data or None if no task is available.
        """
        async with self.lock:
            try:
                task_data = await asyncio.to_thread(self.client.blpop, queue_name, timeout)
                if task_data:
                    _, task_json = task_data
                    task = json.loads(task_json)
                    logger.info(f"Task popped from queue '{queue_name}': {task}")
                    return task
                else:
                    logger.info(f"No task available in queue '{queue_name}' after {timeout} seconds.")
                    return None
            except (RedisError, json.JSONDecodeError) as e:
                logger.error(f"Error popping task from queue '{queue_name}': {e}")
                return None

    async def task_count(self, queue_name: str) -> int:
        """
        Get the number of tasks in the specified Redis queue.

        :param queue_name: Name of the Redis queue.
        :return: The number of tasks in the queue or 0 if there was an error.
        """
        try:
            count = await asyncio.to_thread(self.client.llen, queue_name)
            logger.info(f"Queue '{queue_name}' contains {count} task(s).")
            return count
        except RedisError as e:
            logger.error(f"Error getting task count for queue '{queue_name}': {e}")
            return 0

    async def clear_queue(self, queue_name: str) -> bool:
        """
        Clear all tasks from the specified Redis queue.

        :param queue_name: Name of the Redis queue.
        :return: True if the queue was successfully cleared, False otherwise.
        """
        async with self.lock:
            try:
                result = await asyncio.to_thread(self.client.delete, queue_name)
                if result == 1:
                    logger.info(f"Queue '{queue_name}' cleared successfully.")
                    return True
                else:
                    logger.warning(f"Queue '{queue_name}' was already empty or could not be cleared.")
                    return False
            except RedisError as e:
                logger.error(f"Error clearing queue '{queue_name}': {e}")
                return False

    async def setnx(self, key: str, value: str) -> bool:
        """
        Set a Redis key only if it does not already exist (NX option).
        This ensures thread-safe lock operations for tasks.

        :param key: The Redis key to set.
        :param value: The value to set.
        :return: True if the key was successfully set, False if the key already exists.
        """
        try:
            result = await asyncio.to_thread(self.client.setnx, key, value)
            return result
        except RedisError as e:
            logger.error(f"Error setting key '{key}' with value '{value}': {e}")
            return False

    async def expire(self, key: str, time: int) -> bool:
        """
        Set an expiration time for a key in Redis, ensuring it will expire after a set duration.

        :param key: The Redis key to set the expiration for.
        :param time: The expiration time in seconds.
        :return: True if the expiration was successfully set, False otherwise.
        """
        try:
            result = await asyncio.to_thread(self.client.expire, key, time)
            return result
        except RedisError as e:
            logger.error(f"Error setting expiration for key '{key}': {e}")
            return False

    async def acquire_lock(self, task_id: str, lock_duration: int = 300) -> bool:
        """
        Acquire a lock for the given task using Redis' `setnx` to ensure no race conditions.

        :param task_id: Unique identifier for the task.
        :param lock_duration: Duration in seconds for which the lock is held.
        :return: True if the lock was acquired, False otherwise.
        """
        lock_key = f"task_lock:{task_id}"
        if await self.setnx(lock_key, "locked"):
            await self.expire(lock_key, lock_duration)
            logger.info(f"Lock acquired for task '{task_id}' with duration {lock_duration} seconds.")
            return True
        logger.info(f"Lock acquisition failed for task '{task_id}' (already locked).")
        return False

    async def release_lock(self, task_id: str) -> bool:
        """
        Release the lock for the given task in Redis.

        :param task_id: Unique identifier for the task.
        :return: True if the lock was successfully released, False otherwise.
        """
        try:
            result = await asyncio.to_thread(self.client.delete, f"task_lock:{task_id}")
            if result == 1:
                logger.info(f"Lock released for task '{task_id}'.")
                return True
            logger.info(f"Lock for task '{task_id}' was not found or already released.")
            return False
        except RedisError as e:
            logger.error(f"Error releasing lock for task '{task_id}': {e}")
            return False

    async def close(self):
        """
        Close the Redis connection cleanly.
        """
        try:
            await asyncio.to_thread(self.client.close)
            logger.info("Redis connection closed successfully.")
        except RedisError as e:
            logger.error(f"Error closing Redis connection: {e}")