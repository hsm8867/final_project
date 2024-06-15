ERROR_400_MOVIE_NOT_FOUND = "40000"
ERROR_400_MOVIE_NOT_ENOUGH = "40001"
ERROR_400_OUT_OF_BOUNDARY = "40002"


class BaseAPIException(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message


class MovieNotFoundException(BaseAPIException):
    def __init__(self):
        super().__init__(code=ERROR_400_MOVIE_NOT_FOUND, message="Movie not found")


class MovieNotEnoughException(BaseAPIException):
    def __init__(self):
        super().__init__(code=ERROR_400_MOVIE_NOT_ENOUGH, message="Movie not enough")


class OutofBoundDateException(BaseAPIException):
    def __init__(self):
        super().__init__(code=ERROR_400_OUT_OF_BOUNDARY, message="Date Out of boundary")
