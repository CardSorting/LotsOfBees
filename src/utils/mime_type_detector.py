import os
import mimetypes
import magic
from functools import lru_cache
from typing import Dict, Optional


class MimeTypeDetector:
    """
    MimeTypeDetector efficiently handles MIME type detection based on file extension
    and content, offering high performance through caching and streamlined methods.

    Features:
        - Extension-to-MIME mapping for fast lookup.
        - Content-based detection using the `python-magic` library for accuracy.
        - LRU caching for performance optimization, minimizing repeated lookups.
    """

    def __init__(self):
        """Initialize the detector with an extension map and pre-configure MIME types."""
        self._extension_map = self._load_extension_map()

    @staticmethod
    def _load_extension_map() -> Dict[str, str]:
        """
        Load a predefined map of file extensions to their respective MIME types.

        Returns:
            Dict[str, str]: A dictionary mapping file extensions to MIME types.
        """
        return {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.bmp': 'image/bmp',
            '.webp': 'image/webp',
            '.tiff': 'image/tiff',
            '.svg': 'image/svg+xml',
            '.pdf': 'application/pdf',
            '.doc': 'application/msword',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.xls': 'application/vnd.ms-excel',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.ppt': 'application/vnd.ms-powerpoint',
            '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            '.mp3': 'audio/mpeg',
            '.wav': 'audio/wav',
            '.mp4': 'video/mp4',
            '.avi': 'video/x-msvideo',
            '.zip': 'application/zip',
            '.tar': 'application/x-tar',
            '.gz': 'application/gzip',
            '.txt': 'text/plain',
            '.html': 'text/html',
            '.css': 'text/css',
            '.js': 'application/javascript'
        }

    @lru_cache(maxsize=1024)
    def detect(self, file_path: str) -> str:
        """
        Detect the MIME type of a file using extension and content-based detection.

        The detection process prioritizes extension lookup for speed, followed by content
        inspection via `magic` for cases where the extension lookup fails.

        Args:
            file_path (str): Path to the file for MIME type detection.

        Returns:
            str: Detected MIME type.

        Raises:
            FileNotFoundError: If the file does not exist.
            IOError: If the file cannot be read or analyzed.
        """
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Step 1: Try detecting MIME type based on file extension
        mime_type = self._detect_by_extension(file_path)
        if mime_type:
            return mime_type

        # Step 2: Fallback to content-based detection if extension lookup fails
        return self._detect_by_content(file_path)

    def _detect_by_extension(self, file_path: str) -> Optional[str]:
        """
        Detect MIME type based on file extension using a predefined mapping.

        Args:
            file_path (str): Path to the file.

        Returns:
            Optional[str]: Detected MIME type based on extension, or None if not found.
        """
        ext = os.path.splitext(file_path.lower())[1]
        return self._extension_map.get(ext) or mimetypes.guess_type(file_path)[0]

    @staticmethod
    def _detect_by_content(file_path: str) -> str:
        """
        Detect MIME type by analyzing file content using the `magic` library.

        Args:
            file_path (str): Path to the file.

        Returns:
            str: Detected MIME type by analyzing the file content.

        Raises:
            IOError: If the file cannot be read or analyzed.
        """
        try:
            return magic.from_file(file_path, mime=True)
        except Exception as e:
            raise IOError(f"Failed to detect MIME type by content for {file_path}: {e}")

    def add_mime_type(self, extension: str, mime_type: str) -> None:
        """
        Add or update the mapping between a file extension and a MIME type.

        Args:
            extension (str): The file extension (including the leading dot).
            mime_type (str): The corresponding MIME type.

        Raises:
            ValueError: If the extension or MIME type is incorrectly formatted.
        """
        if not extension.startswith('.') or len(extension) < 2:
            raise ValueError("Extension must start with a dot and have at least one character after.")
        if '/' not in mime_type:
            raise ValueError("Invalid MIME type format.")

        # Add to the extension map and clear cache for consistent results
        self._extension_map[extension.lower()] = mime_type
        self.detect.cache_clear()

    def remove_mime_type(self, extension: str) -> None:
        """
        Remove a MIME type mapping based on the file extension.

        Args:
            extension (str): The file extension (including the leading dot).

        Raises:
            KeyError: If the extension is not found in the current mappings.
        """
        extension = extension.lower()
        if extension not in self._extension_map:
            raise KeyError(f"Extension '{extension}' not found in MIME type mappings.")

        del self._extension_map[extension]
        self.detect.cache_clear()

    def get_supported_extensions(self) -> Dict[str, str]:
        """
        Retrieve all supported file extensions and their associated MIME types.

        Returns:
            Dict[str, str]: A dictionary mapping file extensions to MIME types.
        """
        return dict(self._extension_map)

    def clear_cache(self) -> None:
        """Clear the LRU cache for MIME type detection."""
        self.detect.cache_clear()


# Example Usage
if __name__ == "__main__":
    detector = MimeTypeDetector()

    # Detect a file's MIME type
    mime_type = detector.detect("/path/to/file.jpg")
    print(f"MIME type: {mime_type}")

    # Add a new custom MIME type mapping
    detector.add_mime_type(".xyz", "application/xyz")

    # Remove a MIME type mapping
    detector.remove_mime_type(".xyz")

    # Get all supported extensions
    extensions = detector.get_supported_extensions()
    print(extensions)

    # Clear the cache
    detector.clear_cache()