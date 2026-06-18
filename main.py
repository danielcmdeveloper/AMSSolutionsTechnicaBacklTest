import uuid
import logging
from contextlib import asynccontextmanager
from typing import Literal, Dict, Any

from fastapi import FastAPI, BackgroundTasks, HTTPException, status
from pydantic import BaseModel
import httpx

# --- 1. Observabilidad y Logs ---
# Configuramos el logger para dejar rastro de todo lo que ocurre en producción
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- 2. Control de Memoria (Argumento para la entrevista) ---
# NOTA: En un entorno de producción real, este diccionario en memoria generaría un Memory Leak.
# Lo ideal sería sustituir 'requests_db' por un clúster de Redis con un TTL (Time To Live)
# configurado para cada clave, asegurando limpiezas automáticas.
requests_db: Dict[str, Any] = {}

PROVIDER_URL = "http://provider:3001/v1/notify"
API_KEY = "test-dev-2026"

# Cliente HTTP global
http_client: httpx.AsyncClient = None

# --- 3. Connection Pooling (Ciclo de vida de la App) ---
# En lugar de abrir una conexión TCP por cada petición, abrimos una piscina de conexiones
# al arrancar el servidor y la reutilizamos, ahorrando muchísimos recursos.
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=5.0)
    logger.info("HTTP Client iniciado (Connection Pooling activado)")
    yield
    await http_client.aclose()
    logger.info("HTTP Client cerrado de forma segura")

app = FastAPI(lifespan=lifespan)

# --- 4. Tipado Estricto en Pydantic ---
# FastAPI validará automáticamente que 'type' sea SÓLO uno de estos tres valores.
# Si envían "whatsapp", devolverá un error 400 Bad Request sin que programemos nada.
class NotificationRequest(BaseModel):
    to: str
    message: str
    type: Literal["email", "sms", "push"]

async def process_notification_task(req_id: str, payload: dict):
    requests_db[req_id]["status"] = "processing"
    
    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        # Usamos el cliente global (http_client) en lugar de instanciar uno nuevo
        response = await http_client.post(PROVIDER_URL, json=payload, headers=headers)
        
        if response.status_code in (200, 201, 202):
            requests_db[req_id]["status"] = "sent"
            logger.info(f"Notificación {req_id} enviada con éxito. HTTP {response.status_code}")
        else:
            requests_db[req_id]["status"] = "failed"
            logger.error(f"El proveedor rechazó la notificación {req_id}. HTTP {response.status_code}")
            
    except httpx.RequestError as e:
        # Capturamos timeouts y caídas de red específicas
        requests_db[req_id]["status"] = "failed"
        logger.error(f"Error de red contactando al proveedor para {req_id}: {str(e)}")
    except Exception as e:
        # Capturamos cualquier otra eventualidad
        requests_db[req_id]["status"] = "failed"
        logger.error(f"Error crítico inesperado en {req_id}: {str(e)}")

@app.post("/v1/requests", status_code=status.HTTP_201_CREATED)
async def create_request(request: NotificationRequest):
    req_id = str(uuid.uuid4())
    requests_db[req_id] = {
        "id": req_id,
        "status": "queued",
        "payload": request.model_dump() # model_dump() es el estándar moderno en Pydantic v2
    }
    logger.info(f"Nueva solicitud registrada: {req_id}")
    return {"id": req_id}

@app.get("/v1/requests/{req_id}")
async def get_request(req_id: str):
    if req_id not in requests_db:
        logger.warning(f"Consulta de estado fallida. ID no encontrado: {req_id}")
        raise HTTPException(status_code=404, detail="Request not found")
    
    return {
        "id": req_id,
        "status": requests_db[req_id]["status"]
    }

@app.post("/v1/requests/{req_id}/process", status_code=status.HTTP_202_ACCEPTED)
async def process_request(req_id: str, background_tasks: BackgroundTasks):
    if req_id not in requests_db:
        raise HTTPException(status_code=404, detail="Request not found")
    
    if requests_db[req_id]["status"] != "queued":
        return {"message": "Request already processed or processing"}
        
    # Delegamos el envío a la tarea en segundo plano
    background_tasks.add_task(process_notification_task, req_id, requests_db[req_id]["payload"])
    logger.info(f"Lanzando background task para: {req_id}")
    
    return {"message": "Processing started"}