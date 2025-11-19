import requests
import boto3
import uuid
import time
from datetime import datetime


API_BASE = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb"
TABLE_NAME = "TablaSismosIGP"


def _obtener_sismos_ano(year: int):
    """
    Llama al endpoint JSON que usa el IGP para listar los sismos de un año.
    Devuelve la lista en data['data'] o (None, error_text) si algo falla.
    """
    # Parám cache-buster como en el ejemplo PHP que usa el mismo endpoint
    params = {"_": int(time.time() * 1000)}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }

    url = f"{API_BASE}/{year}"
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    resp.raise_for_status()

    try:
        payload = resp.json()
    except ValueError:
        # No se pudo decodificar JSON
        return None, resp.text[:500]

    data = payload.get("data")
    if not isinstance(data, list):
        return None, f"Respuesta JSON sin campo 'data' válido: {payload}"
    return data, None


def lambda_handler(event, context):
    # Año actual (puedes fijarlo a 2025 si tu profe lo pide así)
    year = datetime.utcnow().year

    sismos, error_body = _obtener_sismos_ano(year)
    if sismos is None:
        return {
            "statusCode": 500,
            "body": f"No se pudieron obtener los sismos del año {year}. Detalle: {error_body}"
        }

    # Ordenar de más reciente a más antiguo por fecha_local + hora_local
    # (por si acaso no vienen ya ordenados)
    sismos_ordenados = sorted(
        sismos,
        key=lambda s: (s.get("fecha_local", ""), s.get("hora_local", "")),
        reverse=True,
    )

    # Nos quedamos con los últimos 10
    sismos_top10 = sismos_ordenados[:10]

    # Armar filas con el mismo estilo que tu ejemplo (id + # + campos)
    rows = []
    for idx, s in enumerate(sismos_top10, start=1):
        item = {
            "#": idx,
            "id": str(uuid.uuid4()),  # PK de DynamoDB
            "reporte_sismico": s.get("reporte", ""),
            "referencia": s.get("referencia", ""),
            "fecha_local": s.get("fecha_local", ""),
            "hora_local": s.get("hora_local", ""),
            # magnitud puede venir numérica o string, lo formateamos como string siempre
            "magnitud": str(s.get("magnitud", "")),
        }
        rows.append(item)

    # ====== Guardar en DynamoDB ======
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TABLE_NAME)

    # Eliminar todos los elementos antes de agregar los nuevos
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan.get("Items", []):
            batch.delete_item(
                Key={
                    "id": each["id"],  # PK definida en tu .yml
                }
            )

    # Insertar los nuevos datos
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item=row)

    return {
        "statusCode": 200,
        "body": rows,
    }
