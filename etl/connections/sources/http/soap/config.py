from pydantic import AnyHttpUrl, BaseModel, ConfigDict


class SOAPConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    wsdl_url: AnyHttpUrl
    username: str | None = None
    password: str | None = None
