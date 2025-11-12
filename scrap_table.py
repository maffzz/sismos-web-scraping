import json
import uuid
import boto3
import requests
from datetime import datetime

# DynamoDB
TABLE_NAME = "SismosScraping"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

# Endpoint ArcGIS del IGP
IGP_API_URL = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"

def lambda_handler(event, context):
    try:
        # Consulta: obtener los 10 últimos sismos ordenados por fecha DESC
        params = {
            "where": "1=1",
            "outFields": "*",
            "orderByFields": "Fecha DESC",
            "resultRecordCount": 10,
            "f": "json"
        }
        response = requests.get(IGP_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        features = data.get("features", [])
        if not features:
            raise ValueError("No se encontraron datos de sismos en la respuesta del IGP")

        items = []
        for f in features:
            attrs = f.get("attributes", {})
            item = {
                "id": str(uuid.uuid4()),
                "fecha": attrs.get("Fecha", "N/A"),
                "hora": attrs.get("Hora", "N/A"),
                "magnitud": str(attrs.get("Magnitud", "N/A")),
                "profundidad_km": str(attrs.get("Profundidad", "N/A")),
                "latitud": str(attrs.get("Latitud", "N/A")),
                "longitud": str(attrs.get("Longitud", "N/A")),
                "referencia": attrs.get("Referencia", "N/A"),
                "fuente": "IGP ArcGIS",
                "timestamp_guardado": datetime.utcnow().isoformat()
            }
            table.put_item(Item=item)
            items.append(item)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "tipo": "INFO",
                "mensaje": "Scraping exitoso desde el servicio ArcGIS del IGP",
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
