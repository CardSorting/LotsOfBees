import aiohttp
from typing import Dict, Any, Optional, List
from urllib.parse import urlencode, urlparse
from utils.logger import Logger

class ShopifyService:
    def __init__(self, shop_name: str, admin_api_token: str):
        self.base_url = f"https://{shop_name}.myshopify.com/admin/api/2024-07"
        self.headers = {
            "X-Shopify-Access-Token": admin_api_token,
            "Content-Type": "application/json"
        }
        self.logger = Logger.get_instance("ShopifyService")

    async def _request(self, method: str, endpoint: str, data: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        url = f"{self.base_url}/{endpoint}"
        self.logger.debug(f"Making {method} request to {url} with data: {data}")
        async with aiohttp.ClientSession() as session:
            try:
                async with session.request(method, url, headers=self.headers, json=data) as response:
                    response_text = await response.text()
                    if response.status in [200, 201]:
                        self.logger.info(f"API {method} request to {url} succeeded.")
                        return await response.json()
                    else:
                        self.logger.error(f"Shopify API error ({response.status}): {response_text}")
                        return None
            except aiohttp.ClientError as e:
                self.logger.error(f"Network error during API {method} request to {url}: {str(e)}")
                return None

    async def create_product(
        self,
        title: str,
        body_html: str,
        vendor: str,
        product_type: str,
        images: List[Dict[str, str]],
        variants: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Creates a new product on Shopify.

        :param title: Product title
        :param body_html: Product description in HTML format
        :param vendor: Vendor name
        :param product_type: Type of the product
        :param images: List of image dictionaries with 'src' keys
        :param variants: List of variant dictionaries with price, inventory_quantity, etc.
        :return: JSON response containing product details or None if an error occurred
        """
        product_data = {
            "product": {
                "title": title,
                "body_html": body_html,
                "vendor": vendor,
                "product_type": product_type,
                "images": images,
                "variants": variants
            }
        }
        response = await self._request("POST", "products.json", product_data)
        if response:
            product = response.get("product")
            if product:
                self.logger.info(f"Product '{title}' created successfully with ID: {product['id']}")
                return product
            else:
                self.logger.error(f"Unexpected response format while creating product '{title}'.")
        else:
            self.logger.error(f"Failed to create product '{title}'.")
        return None

    async def upload_product_image(self, product_id: int, image_url: str) -> Optional[Dict[str, Any]]:
        if not self._is_valid_url(image_url):
            self.logger.error(f"Invalid image URL: {image_url}")
            return None

        image_data = {
            "image": {
                "src": image_url
            }
        }
        endpoint = f"products/{product_id}/images.json"
        self.logger.debug(f"Uploading image to product {product_id} with data: {image_data}")
        response = await self._request("POST", endpoint, image_data)
        if response:
            image = response.get("image")
            if image:
                self.logger.info(f"Image uploaded successfully to product ID {product_id} with image ID: {image['id']}")
                return image
            else:
                self.logger.error(f"Unexpected response format while uploading image to product ID {product_id}.")
        else:
            self.logger.error(f"Failed to upload image to product ID {product_id}.")
        return None

    async def update_product(self, product_id: int, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        product_data = {"product": updates}
        response = await self._request("PUT", f"products/{product_id}.json", product_data)
        if response:
            product = response.get("product")
            if product:
                self.logger.info(f"Product ID {product_id} updated successfully.")
                return product
            else:
                self.logger.error(f"Unexpected response format while updating product ID {product_id}.")
        else:
            self.logger.error(f"Failed to update product ID {product_id}.")
        return None

    async def delete_product(self, product_id: int) -> bool:
        response = await self._request("DELETE", f"products/{product_id}.json")
        if response is not None:
            self.logger.info(f"Product ID {product_id} deleted successfully.")
            return True
        else:
            self.logger.error(f"Failed to delete product ID {product_id}.")
            return False

    async def get_product(self, product_id: int) -> Optional[Dict[str, Any]]:
        response = await self._request("GET", f"products/{product_id}.json")
        if response:
            product = response.get("product")
            if product:
                self.logger.info(f"Product ID {product_id} retrieved successfully.")
                return product
            else:
                self.logger.error(f"Unexpected response format while retrieving product ID {product_id}.")
        else:
            self.logger.error(f"Failed to retrieve product ID {product_id}.")
        return None

    async def list_products(self, limit: int = 50, page_info: Optional[str] = None) -> List[Dict[str, Any]]:
        params = {"limit": limit}
        if page_info:
            params["page_info"] = page_info
        query_string = urlencode(params)
        response = await self._request("GET", f"products.json?{query_string}")
        if response:
            products = response.get("products", [])
            self.logger.info(f"Retrieved {len(products)} products.")
            return products
        else:
            self.logger.error("Failed to list products.")
            return []

    async def update_inventory(self, inventory_item_id: int, location_id: int, available: int) -> bool:
        inventory_data = {
            "location_id": location_id,
            "inventory_item_id": inventory_item_id,
            "available": available
        }
        response = await self._request("POST", "inventory_levels/set.json", inventory_data)
        if response:
            self.logger.info(f"Inventory for item ID {inventory_item_id} updated successfully.")
            return True
        else:
            self.logger.error(f"Failed to update inventory for item ID {inventory_item_id}.")
            return False

    async def get_inventory_levels(self, inventory_item_ids: List[int]) -> Optional[Dict[str, Any]]:
        params = {"inventory_item_ids": ",".join(map(str, inventory_item_ids))}
        query_string = urlencode(params)
        response = await self._request("GET", f"inventory_levels.json?{query_string}")
        if response:
            self.logger.info(f"Retrieved inventory levels for item IDs {inventory_item_ids}.")
            return response.get("inventory_levels")
        else:
            self.logger.error(f"Failed to retrieve inventory levels for item IDs {inventory_item_ids}.")
            return None

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        parsed_url = urlparse(url)
        return all([parsed_url.scheme, parsed_url.netloc])