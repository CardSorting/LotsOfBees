import os
import asyncio
import uuid
from typing import List, Dict, Any, Optional, Tuple
import discord
from discord import app_commands, Interaction
from discord.ext import commands
from PIL import Image
import io
import aiohttp

from redis import Redis
from handlers.super_image_handler import SuperImageHandler
from utils.embed_creator import EmbedCreator
from utils.logger import Logger
from utils.redis_queue import RedisQueue


class Dream(commands.Cog):
    """
    Cog for handling AI-powered dream generation and Shopify integration.
    """

    IMAGE_COUNT: int = 2
    UPDATE_INTERVAL: int = 5  # Seconds
    MAX_UPDATE_ATTEMPTS: int = 60

    def __init__(self, bot: 'DiscordDreamBot', redis_client: Redis):
        """
        Initializes the Dream cog with bot components and Redis client.
        """
        self.bot = bot
        self.image_handler: SuperImageHandler = bot.image_handler
        self.redis_queue: RedisQueue = bot.redis_queue
        self.embed_creator: EmbedCreator = bot.embed_creator
        self.logger: Logger = bot.logger.getChild("Dream")
        self.image_cache: Dict[str, List[Dict[str, Any]]] = {}
        self.active_messages: Dict[str, discord.Message] = {}

    @app_commands.command(name="dream", description="Generate AI-powered art based on a prompt.")
    async def dream(self, interaction: Interaction, prompt: str) -> None:
        """
        Command handler for generating AI-powered art based on a user's prompt.
        """
        await interaction.response.defer()
        self.logger.info(f"User {interaction.user.id} invoked /dream with prompt: {prompt}")

        try:
            images, session_id = await self._generate_images(prompt)
            if not images:
                await interaction.followup.send("Failed to generate images. Please try again later.")
                return

            combined_image_url = await self._create_combined_image(images, session_id)
            embed = await self._create_image_embed(images, prompt, combined_image_url)
            view = DreamView(self, session_id)
            message = await interaction.followup.send(embed=embed, view=view)

            if message:
                self.active_messages[session_id] = message

            # Start periodic updates for queued images
            self.bot.loop.create_task(self._update_embed_periodically(session_id, prompt))
        except Exception as e:
            self.logger.error(f"Unexpected error in dream command: {e}", exc_info=True)
            await interaction.followup.send("An unexpected error occurred. Please try again later.", ephemeral=True)

    async def _generate_images(self, prompt: str, count: int = IMAGE_COUNT) -> Tuple[List[Dict[str, Any]], str]:
        """
        Generate multiple images using the image handler based on the provided prompt.
        """
        session_id = self._generate_session_id()
        tasks = [self._generate_single_image(prompt, session_id) for _ in range(count)]
        images = [img for img in await asyncio.gather(*tasks) if img]

        if images:
            for image in images:
                image['prompt'] = prompt
            self.image_cache[session_id] = images
            self.logger.info(f"Generated {len(images)} images for session {session_id}")

        return images, session_id

    async def _generate_single_image(self, prompt: str, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Generate a single image and queue it for upload.
        """
        try:
            # Generate the image using SuperImageHandler
            image_url = await self.image_handler.generate_image(prompt, image_size="landscape_4_3")
            if not image_url:
                raise ValueError("Image generation failed.")

            # Download the generated image as bytes
            image_content = await self.image_handler._download_image(image_url)
            if image_content is None:
                raise ValueError("Failed to download image content.")

            # Generate a unique file name for the image
            file_name = self._generate_file_name("DREAM")

            # Queue the image upload directly (without base64 encoding)
            await self._queue_image_upload(file_name, image_content, session_id)

            return {'url': f"queued://{file_name}", 'file_name': file_name, 'prompt': prompt}
        except Exception as e:
            self.logger.error(f"Error generating image: {e}", exc_info=True)
            return None

    async def _queue_image_upload(self, file_name: str, image_content: bytes, session_id: str) -> None:
        """
        Queue the image for upload, storing raw binary content in Redis.
        """
        try:
            task_id = str(uuid.uuid4())
            task_data = {
                'id': task_id,
                'file_name': file_name,
                'image_content': image_content,  # Raw binary data
                'session_id': session_id
            }
            await self.redis_queue.push_task('image_upload_queue', task_data)
            self.logger.info(f"Queued image upload for {file_name} in session {session_id}")
        except Exception as e:
            self.logger.error(f"Failed to queue image upload for {file_name}: {e}", exc_info=True)
            raise

    async def _create_combined_image(self, images: List[Dict[str, Any]], session_id: str) -> str:
        """
        Combine multiple images into a single image and upload it.
        """
        try:
            # Fetch image contents concurrently
            image_contents = await asyncio.gather(*[self._fetch_image_content(image['url']) for image in images])
            # Filter out None values
            valid_contents = [content for content in image_contents if content]

            if not valid_contents:
                raise ValueError("No valid image content to combine.")

            # Create a new image canvas
            combined_image = Image.new('RGB', (400 * len(valid_contents), 400))
            for i, content in enumerate(valid_contents):
                img = Image.open(io.BytesIO(content)).resize((400, 400), Image.LANCZOS)
                combined_image.paste(img, (i * 400, 0))

            # Upload the combined image
            combined_image_url = await self._upload_combined_image(combined_image, session_id)
            self.logger.info(f"Combined image uploaded for session {session_id}: {combined_image_url}")
            return combined_image_url
        except Exception as e:
            self.logger.error(f"Error combining images: {e}", exc_info=True)
            return ""

    async def _fetch_image_content(self, url: str) -> Optional[bytes]:
        """
        Fetch the image content from a given URL.
        """
        if url.startswith("queued://"):
            # Image is still queued for upload
            self.logger.debug(f"Image {url} is still queued for upload.")
            return None
        return await self._download_image(url)

    async def _download_image(self, url: str) -> Optional[bytes]:
        """
        Download an image from the provided URL.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        self.logger.info(f"Downloaded image from {url}")
                        return await resp.read()
                    else:
                        self.logger.error(f"Failed to fetch image from {url}, status code: {resp.status}")
        except Exception as e:
            self.logger.error(f"Error downloading image from {url}: {e}", exc_info=True)
        return None

    async def _upload_combined_image(self, combined_image: Image.Image, session_id: str) -> str:
        """
        Upload the combined image to cloud storage and return the public URL.
        """
        try:
            combined_bytes = io.BytesIO()
            combined_image.save(combined_bytes, format='JPEG')
            combined_bytes.seek(0)

            combined_file_name = f"COMBINED_DREAM_{session_id}.jpg"
            combined_image_url = await self.image_handler.upload_file(combined_file_name, combined_bytes.getvalue())
            return combined_image_url
        except Exception as e:
            self.logger.error(f"Failed to upload combined image for session {session_id}: {e}", exc_info=True)
            return ""

    async def _update_embed_periodically(self, session_id: str, prompt: str) -> None:
        """
        Periodically update the embed as queued images are processed.
        """
        try:
            for attempt in range(1, self.MAX_UPDATE_ATTEMPTS + 1):
                await asyncio.sleep(self.UPDATE_INTERVAL)
                if self._session_is_ready(session_id):
                    await self.update_dream_embed(session_id, prompt)
                    self.logger.info(f"Updated embed for session {session_id} after {attempt} attempts.")
                    break
            else:
                self.logger.warning(f"Max update attempts reached for session {session_id}.")
        except Exception as e:
            self.logger.error(f"Error during embed periodic update for session {session_id}: {e}")

    def _session_is_ready(self, session_id: str) -> bool:
        """
        Check if all images in the session are ready.
        """
        images = self.image_cache.get(session_id, [])
        return all(not img['url'].startswith("queued://") for img in images)

    async def update_dream_embed(self, session_id: str, prompt: str) -> None:
        """
        Update the embed once all images in a session are ready.
        """
        if session_id in self.image_cache and session_id in self.active_messages:
            try:
                images = self.image_cache[session_id]
                message = self.active_messages[session_id]
                combined_image_url = await self._create_combined_image(images, session_id)
                embed = await self._create_image_embed(images, prompt, combined_image_url)
                await message.edit(embed=embed)
                self.logger.info(f"Embed updated for session {session_id}")
            except Exception as e:
                self.logger.error(f"Failed to update embed for session {session_id}: {e}", exc_info=True)

    def _generate_session_id(self) -> str:
        """
        Generate a unique session ID for tracking the image generation process.
        """
        return os.urandom(4).hex()

    def _generate_file_name(self, prefix: str) -> str:
        """
        Generate a unique file name for saving the generated images.
        """
        return f"{prefix}_{os.urandom(8).hex()}.jpg"