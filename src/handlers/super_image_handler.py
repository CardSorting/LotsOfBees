import os
from typing import Dict, Optional, Any, List
import fal_client
import aiohttp
from google.cloud import vision
from botocore.exceptions import BotoCoreError, ClientError
import aioboto3
from utils.logger import Logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class SuperImageHandler:
    """
    SuperImageHandler manages the image lifecycle, handling generation, tagging using Google Vision API,
    and uploading to cloud storage with retry logic and optimized error handling for reliability.
    It directly manages binary image content without using base64 encoding.
    """

    REQUIRED_CONFIG_KEYS = [
        'BACKBLAZE_BUCKET_NAME',
        'BACKBLAZE_KEY_ID',
        'BACKBLAZE_APPLICATION_KEY',
        'BACKBLAZE_ENDPOINT_URL'
    ]

    def __init__(self, config: Dict[str, str]):
        """Initialize with validated configuration settings for cloud storage and APIs."""
        self.config = self._validate_config(config)
        self.logger = Logger.get_instance("SuperImageHandler")
        self.session = aioboto3.Session()
        self.vision_client = self._initialize_vision_client()
        self.fal_api_key = self._initialize_fal_api_key()

    def _validate_config(self, config: Dict[str, str]) -> Dict[str, str]:
        """Ensure required configuration keys are present."""
        missing_keys = [key for key in self.REQUIRED_CONFIG_KEYS if key not in config]
        if missing_keys:
            raise ValueError(f"Missing required config keys: {', '.join(missing_keys)}")
        return config

    def _initialize_vision_client(self) -> vision.ImageAnnotatorClient:
        """Initialize the Google Vision API client."""
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY environment variable is not set.")
        return vision.ImageAnnotatorClient(client_options={"api_key": api_key})

    def _initialize_fal_api_key(self) -> str:
        """Fetch and validate FAL API key from environment."""
        api_key = os.getenv("FAL_KEY")
        if not api_key:
            raise ValueError("FAL_KEY environment variable is not set.")
        return api_key

    # MAIN METHOD HANDLING IMAGE PROCESSING FLOW
    async def process_image(self, task_id: str, prompt: str, image_size: str = "landscape_4_3") -> Dict[str, Any]:
        """Orchestrate image generation, tagging, and uploading."""
        try:
            self.logger.info(f"Processing task {task_id}, generating image for prompt: {prompt}")

            # Step 1: Generate Image
            image_url = await self.generate_image(prompt, image_size)
            if not image_url:
                raise RuntimeError(f"Failed to generate image for task {task_id}")

            # Step 2: Download Image
            image_content = await self.download_image(image_url)
            if not image_content:
                raise RuntimeError(f"Failed to download image for task {task_id}")

            # Step 3: Tag Image
            tags = await self.tag_image(image_content)

            # Step 4: Upload Image
            upload_url = await self.upload_file(f"{task_id}.png", image_content)

            # Return the final result
            return {"success": True, "tags": tags, "upload_url": upload_url}

        except Exception as e:
            self.logger.error(f"Unexpected error processing task {task_id}: {e}", exc_info=True)
            return {"success": False, "error": f"Unexpected error: {str(e)}"}

    # GENERATE IMAGE USING FAL API
    async def generate_image(self, prompt: str, image_size: str) -> Optional[str]:
        """Generate an image using FAL API."""
        try:
            handler = fal_client.submit(
                "fal-ai/fast-sdxl",
                arguments={
                    "prompt": prompt,
                    "image_size": image_size,
                    "num_inference_steps": 28,
                    "guidance_scale": 3.5,
                    "num_images": 1,
                    "enable_safety_checker": True
                }
            )
            result = handler.get()
            if result and 'images' in result and result['images']:
                self.logger.info("Image generated successfully using FAL.")
                return result['images'][0]['url']
            return None
        except Exception as e:
            self.logger.error(f"FAL image generation failed: {str(e)}")
            return None

    # DOWNLOAD IMAGE FROM URL
    async def download_image(self, image_url: str) -> Optional[bytes]:
        """Download an image from a URL."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(image_url) as response:
                    if response.status == 200:
                        self.logger.info(f"Image downloaded successfully from {image_url}")
                        return await response.read()
                    self.logger.error(f"Failed to download image. Status code: {response.status}")
                    return None
        except Exception as e:
            self.logger.error(f"Error downloading image from {image_url}: {str(e)}")
            return None

    # TAG IMAGE USING GOOGLE VISION API
    async def tag_image(self, image_content: bytes) -> List[str]:
        """Tag an image using the Google Vision API."""
        try:
            image = vision.Image(content=image_content)
            response = await asyncio.to_thread(self.vision_client.label_detection, image=image)
            tags = [label.description for label in response.label_annotations]
            self.logger.info(f"Image tagged with: {tags}")
            return tags
        except Exception as e:
            self.logger.error(f"Error tagging image: {str(e)}")
            return []

    # UPLOAD FILE TO CLOUD STORAGE (BACKBLAZE)
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10), retry=retry_if_exception_type((BotoCoreError, ClientError)))
    async def upload_file(self, file_name: str, file_content: bytes) -> Optional[str]:
        """Upload an image file to cloud storage with retries."""
        try:
            mime_type = self._get_mime_type(file_name)
            async with self._get_s3_client() as s3_client:
                await s3_client.put_object(
                    Bucket=self.config['BACKBLAZE_BUCKET_NAME'],
                    Key=file_name,
                    Body=file_content,
                    ContentType=mime_type
                )
            self.logger.info(f"Successfully uploaded {file_name}")
            return self._generate_public_url(file_name)
        except (BotoCoreError, ClientError) as e:
            self.logger.error(f"Error uploading image '{file_name}': {str(e)}")
            return None

    # HELPER METHODS FOR UPLOADING
    def _get_mime_type(self, file_name: str) -> str:
        """Determine the MIME type based on file extension."""
        _, extension = os.path.splitext(file_name.lower())
        mime_types = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.bmp': 'image/bmp',
        }
        return mime_types.get(extension, 'application/octet-stream')

    def _generate_public_url(self, file_name: str) -> str:
        """Generate the public URL for an uploaded file."""
        return f"{self.config['BACKBLAZE_ENDPOINT_URL']}/{self.config['BACKBLAZE_BUCKET_NAME']}/{file_name}"

    def _get_s3_client(self):
        """Create and return an S3 client for cloud interactions."""
        return self.session.client(
            's3',
            aws_access_key_id=self.config['BACKBLAZE_KEY_ID'],
            aws_secret_access_key=self.config['BACKBLAZE_APPLICATION_KEY'],
            endpoint_url=self.config['BACKBLAZE_ENDPOINT_URL']
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()
        if exc_type:
            self.logger.error(f"Error during session: {exc_type.__name__}: {exc_val}")
        self.logger.info("SuperImageHandler session closed")