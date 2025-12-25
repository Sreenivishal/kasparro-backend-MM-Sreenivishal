from fastapi import FastAPI
from api.routes import router
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(title="Crypto ETL API")


Instrumentator().instrument(app).expose(app)

app.include_router(router)