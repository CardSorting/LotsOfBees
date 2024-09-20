from typing import Any, Dict, Optional, Type, ClassVar
from enum import Enum
import traceback
import sys

# Centralized enumeration of error codes for consistent error handling
class ErrorCode(Enum):
    GENERAL_ERROR = "GEN001"
    CONFIGURATION_ERROR = "CFG001"
    DATABASE_ERROR = "DB001"
    API_ERROR = "API001"
    AUTHENTICATION_ERROR = "AUTH001"
    AUTHORIZATION_ERROR = "AZ001"
    VALIDATION_ERROR = "VAL001"
    RESOURCE_NOT_FOUND = "RNF001"
    RESOURCE_ALREADY_EXISTS = "RAE001"
    RATE_LIMIT_EXCEEDED = "RLE001"
    TASK_PROCESSING_ERROR = "TPE001"
    NETWORK_ERROR = "NET001"
    THIRD_PARTY_SERVICE_ERROR = "TPS001"
    FILE_OPERATION_ERROR = "FOE001"
    CONCURRENCY_ERROR = "CON001"
    DATA_INTEGRITY_ERROR = "DIE001"

# Base custom exception providing a highly extensible structure
class BaseCustomException(Exception):
    """
    Base class for all custom exceptions, supporting error codes, additional metadata,
    and detailed traceback logging for enhanced debugging and error management.
    """
    default_code: ClassVar[ErrorCode] = ErrorCode.GENERAL_ERROR
    __slots__ = ('message', 'code', 'data', 'traceback')

    def __init__(self, message: str, code: Optional[ErrorCode] = None, data: Optional[Dict[str, Any]] = None):
        self.message = message
        self.code = code or self.default_code
        self.data = data or {}
        self.traceback = "".join(traceback.format_exception(*sys.exc_info()))
        super().__init__(self.message)

    def __str__(self) -> str:
        error_info = f"[{self.code.value}] {self.message}"
        if self.data:
            error_info += f" - Data: {self.data}"
        return error_info

    def to_dict(self) -> Dict[str, Any]:
        return {
            "error": self.__class__.__name__,
            "code": self.code.value,
            "message": self.message,
            "data": self.data,
            "traceback": self.traceback
        }

    @classmethod
    def from_dict(cls: Type['BaseCustomException'], data: Dict[str, Any]) -> 'BaseCustomException':
        code = ErrorCode(data.get('code', cls.default_code.value))
        return cls(
            message=data.get('message', 'Unknown error'),
            code=code,
            data=data.get('data', {})
        )

# Specialized exceptions with default error codes for context-specific errors
class TaskValidationError(BaseCustomException):
    """Raised when task validation fails."""
    default_code = ErrorCode.TASK_PROCESSING_ERROR

class UploadError(BaseCustomException):
    """Raised when file uploads encounter errors."""
    default_code = ErrorCode.FILE_OPERATION_ERROR

class ConfigurationError(BaseCustomException):
    """Raised for configuration-related issues."""
    default_code = ErrorCode.CONFIGURATION_ERROR

class DatabaseError(BaseCustomException):
    """Raised for database operation failures."""
    default_code = ErrorCode.DATABASE_ERROR

class APIError(BaseCustomException):
    """Raised for API-related errors."""
    default_code = ErrorCode.API_ERROR

class AuthenticationError(BaseCustomException):
    """Raised when authentication errors occur."""
    default_code = ErrorCode.AUTHENTICATION_ERROR

class AuthorizationError(BaseCustomException):
    """Raised when authorization fails."""
    default_code = ErrorCode.AUTHORIZATION_ERROR

class ValidationError(BaseCustomException):
    """Raised for validation errors."""
    default_code = ErrorCode.VALIDATION_ERROR

class ResourceNotFoundError(BaseCustomException):
    """Raised when a requested resource cannot be found."""
    default_code = ErrorCode.RESOURCE_NOT_FOUND

class ResourceAlreadyExistsError(BaseCustomException):
    """Raised when attempting to create a resource that already exists."""
    default_code = ErrorCode.RESOURCE_ALREADY_EXISTS

class RateLimitError(BaseCustomException):
    """Raised when rate limits are exceeded."""
    default_code = ErrorCode.RATE_LIMIT_EXCEEDED

class NetworkError(BaseCustomException):
    """Raised when network issues occur."""
    default_code = ErrorCode.NETWORK_ERROR

class ThirdPartyServiceError(BaseCustomException):
    """Raised when third-party service errors occur."""
    default_code = ErrorCode.THIRD_PARTY_SERVICE_ERROR

class FileOperationError(BaseCustomException):
    """Raised for file operation failures."""
    default_code = ErrorCode.FILE_OPERATION_ERROR

class ConcurrencyError(BaseCustomException):
    """Raised for errors related to concurrent operations."""
    default_code = ErrorCode.CONCURRENCY_ERROR

class DataIntegrityError(BaseCustomException):
    """Raised for data integrity violations."""
    default_code = ErrorCode.DATA_INTEGRITY_ERROR