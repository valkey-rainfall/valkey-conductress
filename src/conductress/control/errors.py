"""Stable control-service errors used by domain and HTTP layers."""


class ControlError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


class NotFoundError(ControlError):
    def __init__(self, code: str, message: str):
        super().__init__(code, message, 404)


class ConflictError(ControlError):
    def __init__(self, code: str, message: str):
        super().__init__(code, message, 409)


class AuthorizationError(ControlError):
    def __init__(self, code: str, message: str, status: int = 403):
        super().__init__(code, message, status)
