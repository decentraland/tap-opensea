"""opensea Authentication."""


from singer_sdk.authenticators import SimpleAuthenticator


class openseaAuthenticator(SimpleAuthenticator):
    """Authenticator class for opensea."""

    @classmethod
    def create_for_stream(cls, stream) -> "openseaAuthenticator":
        return cls(
            stream=stream,
            auth_headers={
                "X-API-KEY": stream.config.get("auth_token")
            }
        )
