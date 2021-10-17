import logging

from pydantic import BaseModel, validator

from etl.src.load_methods import InvalidLoadMethod, load_methods


class Request(BaseModel):
    method: str

    @validator("method", pre=True)
    def validate_method(cls, method: str) -> str:
        if method not in load_methods:
            raise InvalidLoadMethod(f"{method} is not in valid methods: {load_methods.keys()}")
        logging.info(f"Data will be written using {method}")
        return method
