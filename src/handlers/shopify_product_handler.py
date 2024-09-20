import logging
from typing import Any, Dict, List, Optional
from aiohttp import ClientSession
from PIL import Image
import io

from services.shopify_service import ShopifyService
from handlers.super_image_handler import SuperImageHandler


class ShopifyProductHandler:
    """
    Handles Shopify product-related tasks, including creating products and updating inventory.
    Integrates image processing (via Google Vision API through SuperImageHandler) and communicates with Shopify API.
    """

    def __init__(self,
                 shopify_client: ShopifyService,
                 image_handler: SuperImageHandler,
                 session: Optional[ClientSession] = None):
        """
        Initialize with required dependencies.

        Args:
            shopify_client (ShopifyService): Shopify API client for product interactions.
            image_handler (SuperImageHandler): Manages image processing (download, tagging, and uploading).
            session (Optional[ClientSession]): Optional HTTP session for external API calls.
        """
        self.shopify_client = shopify_client
        self.image_handler = image_handler
        self.session = session or ClientSession()
        self.logger = logging.getLogger(self.__class__.__name__)

    async def process_task(self, task: Dict[str, Any]) -> None:
        """
        Main entry point for handling a Shopify-related task.

        Args:
            task (Dict[str, Any]): Contains the necessary task data for processing.
        """
        task_id = task.get('id')
        if not task_id:
            self.logger.error("Task must include an 'id' field.")
            return

        try:
            await self._handle_task(task)
        except Exception as e:
            self.logger.error(f"Error processing task {task_id}: {e}", exc_info=True)

    async def _handle_task(self, task: Dict[str, Any]) -> None:
        """
        Core method to handle the flow of processing a task, from downloading an image to adding a product on Shopify.

        Args:
            task (Dict[str, Any]): Task data, including product and image details.
        """
        self._validate_task_data(task)

        image_content = await self.shopify_client.download_image(task['file_name'])  # Assuming the method exists in ShopifyService
        self._ensure_valid_image(task['file_name'], image_content)

        tags = await self._tag_image_content(image_content)
        image_url = await self._upload_image(task['file_name'], image_content)

        product_data = self._prepare_product_data(task['product_data'], tags, image_url)
        await self._submit_product_to_shopify(product_data, task['user_id'])

    def _validate_task_data(self, task: Dict[str, Any]) -> None:
        """
        Validates the task fields to ensure all required information is present.

        Args:
            task (Dict[str, Any]): Task data containing file name, product data, and user ID.

        Raises:
            ValueError: If any required fields are missing.
        """
        required_fields = ['file_name', 'product_data', 'user_id']
        missing_fields = [field for field in required_fields if field not in task]
        if missing_fields:
            raise ValueError(f"Task is missing required fields: {', '.join(missing_fields)}")

        async def _fetch_image_content(self, file_name: str) -> bytes:
                """
                Downloads the image content for a given file name.

                Args:
                    file_name (str): The file name of the image to be downloaded.

                Returns:
                    bytes: The downloaded image content.

                Raises:
                    Exception: If the image download fails.
                """
                try:
                    image_content = await self.image_handler.download_image(file_name)
                    if image_content is None:
                        self.logger.error(f"Downloaded image content is None for file {file_name}")
                        raise ValueError(f"Downloaded image content is None for file {file_name}")
                    return image_content
                except Exception as e:
                    self.logger.error(f"Error downloading image {file_name}: {e}")
                    raise

    def _ensure_valid_image(self, file_name: str, image_content: bytes) -> None:
        """
        Ensures that the downloaded image is valid and can be processed further.

        Args:
            file_name (str): The file name of the image.
            image_content (bytes): The binary content of the downloaded image.

        Raises:
            ValueError: If the image content is not valid.
        """
        if not self._is_image_valid(image_content):
            raise ValueError(f"Invalid image content for file {file_name}")

    def _is_image_valid(self, image_content: bytes) -> bool:
        """
        Validates the image content using PIL to check if it represents a valid image file.

        Args:
            image_content (bytes): The binary image data.

        Returns:
            bool: True if the image is valid, False otherwise.
        """
        try:
            image = Image.open(io.BytesIO(image_content))
            image.verify()
            return True
        except (IOError, SyntaxError):
            return False

    async def _tag_image_content(self, image_content: bytes) -> List[str]:
        """
        Tags the image content using Google Vision API through the SuperImageHandler.

        Args:
            image_content (bytes): The binary content of the image.

        Returns:
            List[str]: A list of descriptive tags for the image.
        """
        try:
            return await self.image_handler.tag_image(image_content)
        except Exception as e:
            self.logger.error(f"Error tagging image content: {e}")
            raise

    async def _upload_image(self, file_name: str, image_content: bytes) -> str:
                """
                Uploads the processed image to cloud storage via SuperImageHandler.

                Args:
                    file_name (str): The name of the image file.
                    image_content (bytes): The binary content of the image.

                Returns:
                    str: The URL of the uploaded image.

                Raises:
                    ValueError: If the upload returns None.
                """
                try:
                    image_url = await self.image_handler.upload_file(file_name, image_content)
                    if image_url is None:
                        self.logger.error(f"Image upload returned None for file {file_name}")
                        raise ValueError(f"Image upload returned None for file {file_name}")
                    return image_url
                except Exception as e:
                    self.logger.error(f"Error uploading image {file_name}: {e}")
                    raise

    def _prepare_product_data(self, product_data: Dict[str, Any], tags: List[str], image_url: str) -> Dict[str, Any]:
        """
        Prepares the product data by adding tags and the uploaded image URL.

        Args:
            product_data (Dict[str, Any]): Original product data.
            tags (List[str]): Image tags generated by the Vision API.
            image_url (str): URL of the uploaded product image.

        Returns:
            Dict[str, Any]: The prepared product data for submission to Shopify.
        """
        product_data.setdefault('tags', []).extend(tags)

        # Ensure that the product contains at least one variant
        product_data['variants'] = product_data.get('variants', [{
            "price": product_data.get("price", "0.00"),
            "sku": product_data.get("sku", "default-sku"),
            "inventory_quantity": product_data.get("inventory_quantity", 1),
            "inventory_management": "shopify"
        }])

        return {
            "product": {
                "title": product_data["title"],
                "body_html": product_data.get("description", ""),
                "vendor": product_data.get("vendor", "default"),
                "product_type": product_data.get("product_type", ""),
                "tags": product_data["tags"],
                "images": [{"src": image_url}],
                "variants": product_data["variants"]
            }
        }

    async def _submit_product_to_shopify(self, product_data: Dict[str, Any], user_id: str) -> None:
        """
        Submits the prepared product data to Shopify.

        Args:
            product_data (Dict[str, Any]): The product data to submit.
            user_id (str): ID of the user creating the product.

        Raises:
            Exception: If there is an error during the product creation process.
        """
        try:
            response = await self.shopify_client.create_product(
                title=product_data['product']['title'],
                body_html=product_data['product']['body_html'],
                vendor=product_data['product']['vendor'],
                product_type=product_data['product']['product_type'],
                images=product_data['product']['images'],
                variants=product_data['product']['variants']
            )

            if response:
                self.logger.info(f"Product '{product_data['product']['title']}' created successfully for user {user_id}.")
            else:
                self.logger.error(f"Failed to create product '{product_data['product']['title']}' for user {user_id}.")
        except Exception as e:
            self.logger.error(f"Error creating product for user {user_id}: {e}")
            raise

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit to close resources."""
        await self.session.close()
        if exc_type:
            self.logger.error(f"Error occurred: {exc_type.__name__}: {exc_val}")
        self.logger.info("ShopifyProductHandler session closed.")