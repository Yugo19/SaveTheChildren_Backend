from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from app.core.logging import logger


class APIException(Exception):
    """Base API exception"""
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail


class ResourceNotFound(APIException):
    def __init__(self, resource: str):
        super().__init__(status.HTTP_404_NOT_FOUND, f"{resource} not found")


class UnauthorizedAccess(APIException):
    def __init__(self):
        super().__init__(status.HTTP_401_UNAUTHORIZED, "Unauthorized")


class ForbiddenAccess(APIException):
    def __init__(self):
        super().__init__(status.HTTP_403_FORBIDDEN, "Insufficient permissions")


class ValidationError(APIException):
    def __init__(self, detail: str):
        super().__init__(status.HTTP_422_UNPROCESSABLE_ENTITY, detail)


def setup_exception_handlers(app: FastAPI):
    @app.exception_handler(APIException)
    async def api_exception_handler(request: Request, exc: APIException):
        logger.error(f"API Exception: {exc.detail}")
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail}
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError):
        errors = exc.errors()
        # Convert bytes to string for JSON serialization
        for error in errors:
            if 'input' in error and isinstance(error['input'], bytes):
                error['input'] = error['input'].decode('utf-8', errors='replace')
        logger.error(f"Validation Error: {errors}")
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": errors}
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(request: Request, exc: Exception):
        logger.error(f"Unhandled Exception: {str(exc)}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"}
        )
