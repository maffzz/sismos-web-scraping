import json
import uuid
import boto3
import requests
from datetime import datetime

# Nombre de la tabla DynamoDB
TABLE_NAME = "SismosScraping"

# Inicializar recurso DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    try:
        # API oficial del IGP
        url = "https://ultimosismo.igp.gob.pe/api/sismo"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Tomar los 10 últimos sismos
        ultimos = data[:10]

        items = []
        for s in ultimos:
            item = {
                "id": str(uuid.uuid4()),
                "fecha_local": s.get("fechaLocal", "N/A"),
                "hora_local": s.get("horaLocal", "N/A"),
                "magnitud": str(s.get("magnitud", "N/A")),
                "profundidad_km": str(s.get("profundidad", "N/A")),
                "latitud": str(s.get("latitud", "N/A")),
                "longitud": str(s.get("longitud", "N/A")),
                "referencia": s.get("referenciaGeografica", "N/A"),
                "fuente": "IGP",
                "timestamp_guardado": datetime.utcnow().isoformat()
            }
            table.put_item(Item=item)
            items.append(item)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "tipo": "INFO",
                "mensaje": "Scraping exitoso desde API dinámica del IGP",
                "cantidad_guardada": len(items),
                "datos": items
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "tipo": "ERROR",
                "mensaje": str(e)
            })
        }