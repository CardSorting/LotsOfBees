import asyncio
from typing import Optional, Dict, Any
from contextlib import AsyncExitStack
import signal
from PIL import Image
import io

from utils.logger import Logger
from utils.redis_queue import RedisQueue
from handlers.super_image_handler import SuperImageHandler
from utils.exceptions import TaskValidationError, UploadError

class RedisUploadWorker:
    """
    A worker class that processes image upload tasks from a Redis queue.
    It uploads the image content (in binary) to cloud storage via SuperImageHandler, 
    and handles retries in case of failure.
    """

    def __init__(self, 
                 redis_queue: RedisQueue, 
                 image_handler: SuperImageHandler, 
                 queue_name: str = 'image_upload_queue',
                 sleep_interval: int = 30,
                 max_retries: int = 3,
                 retry_delay: int = 5):
        """
        Initialize the RedisUploadWorker.

        Args:
            redis_queue (RedisQueue): The Redis queue to pull tasks from.
            image_handler (SuperImageHandler): The handler for uploading images.
            queue_name (str): The Redis queue name.
            sleep_interval (int): Time in seconds to sleep if no task is found.
            max_retries (int): Maximum number of retries for failed uploads.
            retry_delay (int): Delay in seconds between retry attempts.
        """
        self.redis_queue = redis_queue
        self.image_handler = image_handler
        self.queue_name = queue_name
        self.sleep_interval = sleep_interval
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = Logger.get_instance(self.__class__.__name__)
        self.running = asyncio.Event()
        self.tasks = set()

    def _is_valid_image(self, image_content: bytes) -> bool:
        """
        Verify if the provided binary data represents a valid image.

        Args:
            image_content (bytes): The binary content of the image.

        Returns:
            bool: True if the binary data is a valid image, False otherwise.
        """
        try:
            # Try to load the image using PIL to check if it's valid
            image = Image.open(io.BytesIO(image_content))
            image.verify()  # Check if it's a valid image file
            return True
        except (IOError, SyntaxError):
            return False

    async def start(self) -> None:
        """
        Start processing tasks from the Redis queue.
        """
        self.running.set()
        self.logger.info("RedisUploadWorker started and is listening for tasks.")

        async with AsyncExitStack() as stack:
            stack.push_async_callback(self.shutdown)
            stack.push_async_callback(self._cleanup_tasks)

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
                self.running.clear()

    async def _process_next_task(self) -> None:
        """
        Fetch the next task from the queue and process it.
        """
        task = await self.redis_queue.pop_task(self.queue_name)
        if task:
            self.tasks.add(asyncio.create_task(self._process_task(task)))
        else:
            await self._handle_no_task()

    async def _process_task(self, task: Dict[str, Any]) -> None:
        """
        Process a single task from the queue.
        Args:
            task (Dict[str, Any]): The task containing image upload information.
        """
        task_id = task.get('id')
        file_name = task.get('file_name')
        image_content = task.get('image_content')  # Binary data now
        session_id = task.get('session_id')

        if not all([task_id, file_name, image_content, session_id]):
            self.logger.error(f"Invalid task structure: {task}")
            return

        try:
            await self._execute_task(task_id, file_name, image_content)
        except Exception as e:
            self.logger.error(f"Unexpected error processing task {task_id}: {e}", exc_info=True)

    async def _execute_task(self, task_id: str, file_name: str, image_content: bytes) -> None:
        """
        Execute the upload task, including validation and retries.

        Args:
            task_id (str): The ID of the task.
            file_name (str): The name of the file to upload.
            image_content (bytes): Binary image content.
        """
        try:
            self._validate_task(file_name, image_content)
            self.logger.info(f"Processing upload task for {file_name} (Task ID: {task_id})")

            await self._upload_with_retries(task_id, file_name, image_content)

        except TaskValidationError as e:
            self.logger.error(f"Task validation error for task {task_id}: {e}")
        except UploadError as e:
            self.logger.error(f"Upload error for task {task_id}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error executing task {task_id}: {e}", exc_info=True)

    def _validate_task(self, file_name: Optional[str], image_content: Optional[bytes]) -> None:
        """
        Validate the task to ensure it contains the necessary data and that the image is valid.

        Args:
            file_name (Optional[str]): The name of the file.
            image_content (Optional[bytes]): Binary image content.

        Raises:
            TaskValidationError: If any required fields are missing or the image is invalid.
        """
        if not file_name:
            raise TaskValidationError("Task is missing 'file_name'")
        if not image_content:
            raise TaskValidationError("Task is missing 'image_content'")

        # Validate that the binary data represents a valid image
        if not self._is_valid_image(image_content):
            raise TaskValidationError(f"Task is invalid because 'image_content' is not a valid image file.")

    async def _upload_with_retries(self, task_id: str, file_name: str, image_content: bytes) -> None:
        """
        Attempt to upload the image with retries in case of failure.

        Args:
            task_id (str): The ID of the task.
            file_name (str): The name of the file to upload.
            image_content (bytes): The binary image content to upload.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                uploaded_url = await self.image_handler.upload_file(file_name, image_content)
                if uploaded_url:
                    self.logger.info(f"Successfully uploaded {file_name} to {uploaded_url} (Task ID: {task_id})")
                    return
                else:
                    raise UploadError(f"Upload failed without URL for {file_name} (Task ID: {task_id})")
            except Exception as e:
                if attempt < self.max_retries:
                    self.logger.warning(f"Upload attempt {attempt} failed for {file_name} (Task ID: {task_id}). Retrying in {self.retry_delay} seconds...")
                    await asyncio.sleep(self.retry_delay)
                else:
                    self.logger.error(f"All upload attempts failed for {file_name} (Task ID: {task_id}): {e}", exc_info=True)
                    raise UploadError(f"All upload attempts failed for {file_name} (Task ID: {task_id})") from e

    async def _handle_no_task(self) -> None:
        """
        Handle the case where no tasks are found in the queue.
        """
        self.logger.debug(f"No tasks found in '{self.queue_name}'. Sleeping for {self.sleep_interval} seconds.")
        await asyncio.sleep(self.sleep_interval)

    async def stop(self) -> None:
        """
        Stop the RedisUploadWorker.
        """
        self.logger.info("Stopping RedisUploadWorker...")
        self.running.clear()

    async def shutdown(self) -> None:
        """
        Shut down the RedisUploadWorker, cleaning up any remaining tasks.
        """
        self.logger.info("Shutting down RedisUploadWorker...")
        await self._cleanup_tasks()
        self.logger.info("RedisUploadWorker shutdown complete.")

    async def _cleanup_tasks(self) -> None:
        """
        Cancel any running tasks and ensure they are cleaned up.
        """
        if self.tasks:
            self.logger.info(f"Cleaning up {len(self.tasks)} running tasks...")
            for task in self.tasks:
                task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)
            self.tasks.clear()

    @classmethod
    async def create_and_run(cls, *args, **kwargs) -> None:
        """
        Create an instance of RedisUploadWorker and run it, handling shutdown signals.
        """
        worker = cls(*args, **kwargs)

        def signal_handler():
            asyncio.create_task(worker.stop())

        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                asyncio.get_running_loop().add_signal_handler(sig, signal_handler)

            await worker.start()
        finally:
            for sig in (signal.SIGINT, signal.SIGTERM):
                asyncio.get_running_loop().remove_signal_handler(sig)