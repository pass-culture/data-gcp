from fastapi import FastAPI
from loguru import logger

from app.routes import api

app = FastAPI()

app.include_router(api)


if __name__ == "__main__":
    import uvicorn

    logger.info("startup", extra={"event": "startup", "response": "ready"})
    uvicorn.run(app, host="0.0.0.0", port=8080)
