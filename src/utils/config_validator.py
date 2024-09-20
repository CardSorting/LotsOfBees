from typing import Dict, List, Any, Callable, Optional
import re
from urllib.parse import urlparse


class ConfigValidationError(Exception):
    """Custom exception for configuration validation errors."""
    pass


class ConfigValidator:
    """
    A utility class for validating configuration dictionaries.

    This class provides methods to ensure that a configuration dictionary
    contains all required keys and that the values meet specified criteria.
    """

    @staticmethod
    def validate(config: Dict[str, Any], required_keys: List[str], 
                 custom_validators: Optional[Dict[str, Callable[[Any], bool]]] = None) -> Dict[str, Any]:
        """
        Validate the provided configuration dictionary.

        Args:
            config (Dict[str, Any]): The configuration dictionary to validate.
            required_keys (List[str]): List of keys that must be present in the config.
            custom_validators (Optional[Dict[str, Callable[[Any], bool]]]): 
                Optional dictionary of custom validation functions for specific keys.

        Returns:
            Dict[str, Any]: The validated configuration dictionary.

        Raises:
            ConfigValidationError: If the configuration fails validation.
        """
        ConfigValidator._check_required_keys(config, required_keys)
        ConfigValidator._validate_values(config, custom_validators or {})
        return config

    @staticmethod
    def _check_required_keys(config: Dict[str, Any], required_keys: List[str]) -> None:
        """
        Check if all required keys are present in the configuration.

        Args:
            config (Dict[str, Any]): The configuration dictionary to check.
            required_keys (List[str]): List of keys that must be present.

        Raises:
            ConfigValidationError: If any required keys are missing.
        """
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise ConfigValidationError(f"Missing required configuration keys: {', '.join(missing_keys)}")

    @staticmethod
    def _validate_values(config: Dict[str, Any], custom_validators: Dict[str, Callable[[Any], bool]]) -> None:
        """
        Validate the values in the configuration using custom validators.

        Args:
            config (Dict[str, Any]): The configuration dictionary to validate.
            custom_validators (Dict[str, Callable[[Any], bool]]): 
                Dictionary of custom validation functions for specific keys.

        Raises:
            ConfigValidationError: If any values fail validation.
        """
        for key, validator in custom_validators.items():
            if key in config:
                if not validator(config[key]):
                    raise ConfigValidationError(f"Invalid value for key '{key}': {config[key]}")

    @staticmethod
    def is_non_empty_string(value: Any) -> bool:
        """Check if the value is a non-empty string."""
        return isinstance(value, str) and len(value.strip()) > 0

    @staticmethod
    def is_positive_integer(value: Any) -> bool:
        """Check if the value is a positive integer."""
        return isinstance(value, int) and value > 0

    @staticmethod
    def is_valid_url(value: Any) -> bool:
        """Check if the value is a valid URL."""
        try:
            result = urlparse(value)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    @staticmethod
    def is_valid_email(value: Any) -> bool:
        """Check if the value is a valid email address."""
        email_regex = re.compile(r"[^@]+@[^@]+\.[^@]+")
        return isinstance(value, str) and email_regex.match(value) is not None

    @staticmethod
    def is_boolean(value: Any) -> bool:
        """Check if the value is a boolean."""
        return isinstance(value, bool)

    @staticmethod
    def is_list_of_strings(value: Any) -> bool:
        """Check if the value is a list of strings."""
        return isinstance(value, list) and all(isinstance(item, str) for item in value)

    @staticmethod
    def is_dict(value: Any) -> bool:
        """Check if the value is a dictionary."""
        return isinstance(value, dict)

    @staticmethod
    def create_range_validator(min_value: float, max_value: float) -> Callable[[Any], bool]:
        """
        Create a validator function for checking if a value is within a specified range.

        Args:
            min_value (float): The minimum allowed value.
            max_value (float): The maximum allowed value.

        Returns:
            Callable[[Any], bool]: A function that validates if a value is within the specified range.
        """
        def validator(value: Any) -> bool:
            return isinstance(value, (int, float)) and min_value <= value <= max_value
        return validator

    @staticmethod
    def create_regex_validator(pattern: str) -> Callable[[Any], bool]:
        """
        Create a validator function for checking if a string matches a specified regex pattern.

        Args:
            pattern (str): The regex pattern to match against.

        Returns:
            Callable[[Any], bool]: A function that validates if a string matches the specified pattern.
        """
        compiled_pattern = re.compile(pattern)
        def validator(value: Any) -> bool:
            return isinstance(value, str) and compiled_pattern.match(value) is not None
        return validator