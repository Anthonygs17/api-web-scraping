import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página web que contiene la tabla de sismos
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    # Realizar la solicitud HTTP a la página web
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear el contenido HTML de la página web
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar la primera tabla en el HTML
    table = soup.find('table')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla en la página web'
        }

    # (Opcional) Extraer los encabezados de la tabla
    # Los usamos solo como referencia, pero mapeamos a nombres fijos
    headers = [header.get_text(strip=True) for header in table.find_all('th')]

    # Extraer las filas de la tabla (omitimos el encabezado)
    rows_html = table.find_all('tr')[1:]

    # Solo queremos los últimos 10 sismos (la tabla suele estar ordenada del más reciente al más antiguo)
    rows = []
    for idx, row in enumerate(rows_html[:10], start=1):
        cells = row.find_all('td')
        # Nos aseguramos de que haya al menos 4 columnas (reporte, referencia, fecha/hora, magnitud)
        if len(cells) < 4:
            continue

        # Limpiamos el texto de cada celda
        reporte_sismico = cells[0].get_text(strip=True)
        referencia = cells[1].get_text(strip=True)
        fecha_hora_local = cells[2].get_text(strip=True)
        magnitud = cells[3].get_text(strip=True)

        # Construimos el diccionario siguiendo el estilo de tu ejemplo
        item = {
            '#': idx,  # índice 1..10
            'id': str(uuid.uuid4()),  # ID único para la PK en DynamoDB
            'reporte_sismico': reporte_sismico,
            'referencia': referencia,
            'fecha_hora_local': fecha_hora_local,
            'magnitud': magnitud
        }
        rows.append(item)

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    # Cambia el nombre de la tabla según lo que tengas creado en AWS
    table = dynamodb.Table('TablaSismosIGP')

    # Eliminar todos los elementos de la tabla antes de agregar los nuevos
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan.get('Items', []):
            batch.delete_item(
                Key={
                    'id': each['id']  # Se asume que 'id' es la partition key (String)
                }
            )

    # Insertar los nuevos datos (últimos 10 sismos)
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item=row)

    # Retornar el resultado como JSON (Lambda-style)
    return {
        'statusCode': 200,
        'body': rows
    }
