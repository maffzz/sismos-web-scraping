import json
import uuid
import boto3
import requests
from datetime import datetime, timedelta

# Nombre de la tabla DynamoDB
TABLE_NAME = "SismosScraping"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

# Endpoint del IGP (ArcGIS REST API)
IGP_API_URL = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"

def convertir_fecha(fecha_millis):
    """Convierte la fecha del formato milisegundos (ArcGIS) a ISO"""
    try:
        return (datetime(1970, 1, 1) + timedelta(milliseconds=fecha_millis)).isoformat()
    except Exception:
        return "N/A"

def lambda_handler(event, context):
    try:
        # Consulta de los últimos 10 sismos (ordenados por objectid)
        params = {
            "where": "1=1",
            "outFields": "*",
            "orderByFields": "objectid DESC",
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

            fecha_iso = convertir_fecha(attrs.get("fecha"))
            item = {
                "id": str(uuid.uuid4()),
                "fecha": fecha_iso,
                "hora": attrs.get("hora", "N/A"),
                "magnitud": str(attrs.get("magnitud", attrs.get("mag", "N/A"))),
                "profundidad_km": str(attrs.get("prof", "N/A")),
                "clasificacion_profundidad": attrs.get("profundidad", "N/A"),
                "latitud": str(attrs.get("lat", "N/A")),
                "longitud": str(attrs.get("lon", "N/A")),
                "referencia": attrs.get("ref", "N/A"),
                "departamento": attrs.get("departamento", "N/A"),
                "codigo_evento": attrs.get("code", "N/A"),
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
                "mensaje": "Scraping exitoso con datos reales del IGP",
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
