import discord
from discord import Embed, Colour
from typing import Optional, List, Tuple

class EmbedCreator:
    """
    The magical factory for creating beautiful, vibrant Discord embeds! 🌟
    Whether you need an informative box, a product spotlight, or a fiery error message,
    this class can craft them with ease and style! 🎨✨
    """

    def __init__(self, default_color: Colour = Colour.blue()):
        """
        Initialize the EmbedCreator with a default color. Think of it as choosing your favorite crayon! 🖍️

        :param default_color: The go-to color for your embeds, default is a cool blue. 💙
        """
        self.default_color = default_color

    def create_embed(
        self, 
        title: str, 
        description: Optional[str] = None, 
        color: Optional[Colour] = None, 
        image_url: Optional[str] = None, 
        footer_text: Optional[str] = None, 
        fields: Optional[List[Tuple[str, str, bool]]] = None
    ) -> Embed:
        """
        Assemble a glorious embed with all the bells and whistles! 🎉

        :param title: The crown jewel, the title of the embed! 👑
        :param description: The captivating text beneath the title (optional, but fancy!).
        :param color: Splash on some color! Defaults to your chosen default or a calming blue. 🎨
        :param image_url: A picture's worth a thousand words—add one to your embed (optional)! 🖼️
        :param footer_text: Pop in a little footer text to end on a high note (optional). 🎶
        :param fields: Add nifty fields in a tuple format (name, value, inline), perfect for info-rich embeds! 📊
        :return: A Discord Embed that's sure to impress! 🎁
        """
        embed = Embed(title=title, description=description, color=color or self.default_color)

        if image_url:
            embed.set_image(url=image_url)

        if footer_text:
            embed.set_footer(text=footer_text)

        if fields:
            for name, value, inline in fields:
                embed.add_field(name=name, value=value, inline=inline)

        return embed

    def create_image_embed(self, image_url: str, title: str, description: Optional[str] = None) -> Embed:
        """
        Roll out the red carpet for your images! 🎬
        This embed is tailor-made to show off your image in style.

        :param image_url: The starring image's URL! 🌟
        :param title: The snazzy title of the embed.
        :param description: Add a little context for your image, if you like (optional).
        :return: A sleek, image-focused Discord Embed! 🖼️✨
        """
        return self.create_embed(title=title, description=description, image_url=image_url)

    def create_confirmation_embed(self, title: str, description: str) -> Embed:
        """
        A confirmation message wrapped in a crisp green color. It's like getting a virtual thumbs-up! 👍

        :param title: The title to celebrate your success! 🎉
        :param description: A little note confirming the good news.
        :return: A green-colored Discord Embed, the color of triumph! 💚
        """
        return self.create_embed(title=title, description=description, color=Colour.green())

    def create_error_embed(self, title: str, description: str) -> Embed:
        """
        For when things go a bit sideways... Give your error messages the dramatic red they deserve! 🚨

        :param title: The title to declare something went wrong. ⚠️
        :param description: A little more info on what happened.
        :return: A red-colored Discord Embed, because even errors can look sharp! 🔴
        """
        return self.create_embed(title=title, description=description, color=Colour.red())

    def create_info_embed(self, title: str, description: str, footer_text: Optional[str] = None, image_url: Optional[str] = None) -> Embed:
        """
        Sharing some news? Use this trusty info box to get the word out! 📰

        :param title: The headline! 🎤
        :param description: The full scoop, packed with all the details!
        :param footer_text: A little footer text to add a final note (optional).
        :param image_url: An image to jazz up the info box (optional).
        :return: A blue-colored Discord Embed, perfect for informative announcements! 💙
        """
        return self.create_embed(title=title, description=description, footer_text=footer_text, image_url=image_url)

    def create_product_embed(self, title: str, description: str, image_url: str, price: str, vendor: str) -> Embed:
        """
        Showcase your product with an embed that pops! 🎁 Add a little sparkle to the display.

        :param title: The product's name, loud and proud! 🎩
        :param description: A fun description that sells the story behind the product.
        :param image_url: A snazzy image to make your product shine! 💎
        :param price: The all-important price tag! 💲
        :param vendor: Who’s the master behind this creation? Let the vendor have their spotlight! 🌟
        :return: A product display embed with fields for price and vendor! 🛍️
        """
        fields = [
            ("Price", price, True),
            ("Vendor", vendor, True)
        ]
        return self.create_embed(title=title, description=description, image_url=image_url, fields=fields)

    def create_warning_embed(self, title: str, description: str) -> Embed:
        """
        Time to flash those warning lights! 🚧 A bold orange box to get everyone's attention.

        :param title: The headline to call attention to the warning! 🛑
        :param description: The details of what to watch out for.
        :return: An orange-colored Discord Embed, perfect for cautionary tales! 🍊
        """
        return self.create_embed(title=title, description=description, color=Colour.orange())

    def create_action_embed(self, title: str, description: str, action_text: str, action_url: str) -> Embed:
        """
        Ready, set, action! ⚡ This embed comes with a clickable action link to get things moving.

        :param title: The title, ready for action! 💥
        :param description: A call-to-action or some descriptive text.
        :param action_text: The text for the action link.
        :param action_url: The URL the action link should lead to.
        :return: A Discord Embed with an action link field! 🚀
        """
        fields = [("Action", f"[{action_text}]({action_url})", False)]
        return self.create_embed(title=title, description=description, fields=fields)