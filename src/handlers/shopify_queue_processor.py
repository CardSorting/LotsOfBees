import asyncio
import logging
import json
from typing import Dict, Any, Optional
from redis import Redis
from redis.exceptions import RedisError
from utils.redis_queue import RedisQueue
from handlers.shopify_product_handler import ShopifyProductHandler

class ShopifyQueueProcessor:
    """
    ShopifyQueueProcessor is responsible for managing the task queue in Redis and delegating
    tasks to the ShopifyProductHandler for processing. This class ensures proper handling
    of tasks fetched from the queue, including error logging and graceful shutdown.
    """
    def __init__(self, 
                 redis_client: Redis, 
                 product_handler: ShopifyProductHandler, 
                 queue_name: str, 
                 logger: Optional[logging.Logger] = None,
                 sleep_interval: int = 1):
        """
        Initialize the ShopifyQueueProcessor with Redis client and ShopifyProductHandler.

        Args:
            redis_client (Redis): Redis client used for managing the task queue.
            product_handler (ShopifyProductHandler): Instance responsible for processing tasks.
            queue_name (str): Name of the Redis queue to pull tasks from.
            logger (Optional[logging.Logger]): Optional logger for logging. Defaults to a class-specific logger.
            sleep_interval (int): Time to sleep between queue checks when no tasks are available.
        """
        self.redis = redis_client
        self.product_handler = product_handler
        self.queue_name = queue_name
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.sleep_interval = sleep_interval
        self.redis_queue = RedisQueue()
        self.running = asyncio.Event()

    async def start(self):
        """
        Start continuously fetching and processing tasks from the Redis queue.
        """
        self.logger.info(f"Starting ShopifyQueueProcessor on queue: {self.queue_name}")
        self.running.set()
        try:
            while self.running.is_set():
                try:
                    await self._process_next_task()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.logger.error(f"Unexpected error in task loop: {e}", exc_info=True)
                    await asyncio.sleep(1)  # Prevent tight error loop
        finally:
            await self.stop()

    async def _process_next_task(self):
        """
        Fetch and process the next task from the queue.
        """
        task = await self._get_next_task()
        if task:
            await self.product_handler.process_task(task)
        else:
            await asyncio.sleep(self.sleep_interval)

    async def _get_next_task(self) -> Optional[Dict[str, Any]]:
        """
        Fetch the next task from the Redis queue.

        Returns:
            Optional[Dict[str, Any]]: The task to be processed, or None if no task is available.
        """
        try:
            task_data = await asyncio.to_thread(self.redis.blpop, self.queue_name, timeout=1)
            if task_data:
                _, task_json = task_data
                task = json.loads(task_json)
                self.logger.info(f"Task retrieved from queue '{self.queue_name}': {task}")
                return task
            return None
        except RedisError as e:
            self.logger.error(f"Error retrieving task from queue '{self.queue_name}': {e}")
            return None

    async def stop(self):
        """
        Gracefully stop the worker by shutting down the Redis connection.
        """
        self.logger.info("Shutting down ShopifyQueueProcessor.")
        self.running.clear()
        try:
            await asyncio.to_thread(self.redis.close)
            self.logger.info("Redis connection closed successfully.")
        except RedisError as e:
            self.logger.error(f"Error closing Redis connection: {e}")