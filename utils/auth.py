import base64


class Authorization:
    def __init__(
        self, api_key: str, api_user: str, api_password: str, logger: object
    ) -> None:
        self.api_key = api_key
        self.api_user = api_user
        self.api_password = api_password
        self.logger = logger

    def get_auth_header(self) -> str:
        auth_type = "Unset"
        auth_value = ""
        if self.api_key:
            auth_type = "Bearer Token"
            auth_value = f"Bearer {self.api_key}"
        elif self.api_user and self.api_password:
            auth_type = "Basic Auth"
            auth_str = f"{self.api_user}:{self.api_password}"
            b64_auth_str = base64.b64encode(bytes(auth_str, "utf-8")).decode("utf-8")
            auth_value = f"Basic {b64_auth_str}"
        self.logger.info("Authorization Type: %s", auth_type)
        return auth_value
