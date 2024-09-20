import asyncio
import os
from typing import Any, Dict, List
import discord
from discord.ext import commands
from redis import Redis
from dream_cog import Dream
from handlers.shopify_product_handler import ShopifyProductHandler
from handlers.redis_upload_worker import RedisUploadWorker
from handlers.super_image_handler import SuperImageHandler
from services.shopify_service import ShopifyService
from utils.embed_creator import EmbedCreator
from utils.logger import Logger
from utils.redis_queue import RedisQueue
from handlers.shopify_queue_processor import ShopifyQueueProcessor

class DiscordDreamBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix='!', intents=intents)

        self.logger = Logger.get_instance("DiscordDreamBot")
        self.embed_creator = EmbedCreator()
        self.redis_queue = RedisQueue()

        self.redis_client = self._initialize_redis_client()

        self.image_handler = self._initialize_image_handler()

        self.shopify_service = self._initialize_shopify_service()
        self.shopify_product_handler = ShopifyProductHandler(
            shopify_client=self.shopify_service,
            image_handler=self.image_handler)

        self.shopify_queue_processor = ShopifyQueueProcessor(
            redis_client=self.redis_client,
            queue_name="shopify_product_queue",
            logger=self.logger,
            product_handler=self.shopify_product_handler
        )

        self.redis_upload_worker = RedisUploadWorker(
            redis_queue=self.redis_queue,
            image_handler=self.image_handler,
            sleep_interval=30
        )

        self.image_cache: Dict[str, List[Dict[str, Any]]] = {}

    def _initialize_redis_client(self) -> Redis:
        redis_host = os.getenv('REDIS_HOST', 'localhost')
        redis_port = os.getenv('REDIS_PORT', '6379')
        redis_password = os.getenv('REDIS_PASSWORD', None)

        return Redis(
            host=redis_host,
            port=int(redis_port),
            password=redis_password,
            decode_responses=True
        )

    def _initialize_image_handler(self) -> SuperImageHandler:
        image_handler_config = {
            'FAL_KEY': os.getenv('FAL_KEY'),
            'BACKBLAZE_BUCKET_NAME': os.getenv('BACKBLAZE_BUCKET_NAME'),
            'BACKBLAZE_KEY_ID': os.getenv('BACKBLAZE_KEY_ID'),
            'BACKBLAZE_APPLICATION_KEY': os.getenv('BACKBLAZE_APPLICATION_KEY'),
            'BACKBLAZE_ENDPOINT_URL': os.getenv('BACKBLAZE_ENDPOINT_URL'),
        }

        image_handler_config = {k: v for k, v in image_handler_config.items() if v is not None}

        return SuperImageHandler(config=image_handler_config)

    def _initialize_shopify_service(self) -> ShopifyService:
        return ShopifyService(
            shop_name=os.getenv('SHOPIFY_SHOP_NAME'),
            admin_api_token=os.getenv('SHOPIFY_ADMIN_API_TOKEN'))

    async def setup_hook(self) -> None:
        await self.add_cog(Dream(self, self.redis_client))
        await self.tree.sync()
        self.logger.info('Synced dream command globally.')

        self.loop.create_task(self.redis_upload_worker.start())
        self.loop.create_task(self._product_worker_task())

    async def _product_worker_task(self) -> None:
        while True:
            try:
                await self.shopify_queue_processor.start()
            except Exception as e:
                self.logger.error(f"Error processing product task: {e}", exc_info=True)
            await asyncio.sleep(5)  # Prevent tight loop if continuous errors occur

    async def on_ready(self) -> None:
        self.logger.info(f'DreamBot connected as {self.user}')

    async def close(self) -> None:
        await super().close()
        await self.redis_queue.close()
        await self.redis_upload_worker.stop()
        self.logger.info("Bot shutdown completed.")

async def main() -> None:
    bot = DiscordDreamBot()

    try:
        await bot.start(os.getenv('DISCORD_TOKEN'))
    except KeyboardInterrupt:
        bot.logger.info("Bot interrupted by user. Shutting down...")
    except Exception as e:
        bot.logger.error(f"Unexpected error occurred: {e}", exc_info=True)
    finally:
        await bot.close()

if __name__ == '__main__':
    asyncio.run(main())