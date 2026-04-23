from rest_framework.views import exception_handler


def custom_exception_handler(exc, context):
    response = exception_handler(exc, context)
    if response is not None and isinstance(response.data, dict):
        if "detail" in response.data and "code" not in response.data:
            code = getattr(exc, "default_code", "error")
            response.data = {**response.data, "code": code}
    return response
