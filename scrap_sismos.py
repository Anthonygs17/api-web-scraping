import uuid
import boto3

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Si usas webdriver-manager localmente, puedes descomentar esto:
# from webdriver_manager.chrome import ChromeDriverManager


def lambda_handler(event, context):
    # URL de la página web que contiene la tabla de sismos
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    # ==============================
    #   1. Configurar Selenium
    # ==============================
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # En Lambda con imagen propia: normalmente usas Service("/path/to/chromedriver")
    # En local, podrías usar ChromeDriverManager().install()
    #
    # Ejemplo LOCAL:
    # service = Service(ChromeDriverManager().install())
    #
    # Ejemplo genérico (ajusta el path del driver según tu entorno Lambda):
    service = Service("/opt/chromedriver")  # <-- CAMBIA esto si usas otra ruta/imagen

    driver = webdriver.Chrome(service=service, options=options)

    try:
        # ==============================
        #   2. Abrir página y esperar tabla
        # ==============================
        driver.get(url)

        # Esperar a que exista al menos una fila de tabla con celdas <td>
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located(
                    (By.XPATH, "//table//tr[td]")
                )
            )
        except Exception:
            # Si no encontró la tabla o se demoró demasiado
            return {
                'statusCode': 404,
                'body': 'No se encontró la tabla de sismos en la página web'
            }

        filas = driver.find_elements(By.XPATH, "//table//tr[td]")

        # ==============================
        #   3. Extraer solo los últimos 10 sismos
        # ==============================
        rows = []
        for idx, fila in enumerate(filas[:10], start=1):
            celdas = fila.find_elements(By.TAG_NAME, "td")
            if len(celdas) < 4:
                # Por si hay alguna fila rara
                continue

            reporte_sismico = celdas[0].text.strip()
            referencia = celdas[1].text.strip()
            fecha_hora_local = celdas[2].text.strip()
            magnitud = celdas[3].text.strip()

            item = {
                '#': idx,
                'id': str(uuid.uuid4()),          # PK de DynamoDB
                'reporte_sismico': reporte_sismico,
                'referencia': referencia,
                'fecha_hora_local': fecha_hora_local,
                'magnitud': magnitud
            }
            rows.append(item)

        if not rows:
            return {
                'statusCode': 404,
                'body': 'No se pudieron extraer filas de sismos de la tabla'
            }

        # ==============================
        #   4. Guardar en DynamoDB
        # ==============================
        dynamodb = boto3.resource('dynamodb')
        table_name = 'TablaSismosIGP'   # 👈 nombre que definiste en el .yml
        table = dynamodb.Table(table_name)

        # Borrar todos los elementos antes de insertar los nuevos
        scan = table.scan()
        with table.batch_writer() as batch:
            for each in scan.get('Items', []):
                batch.delete_item(
                    Key={
                        'id': each['id']  # PK definida en el .yml
                    }
                )

        # Insertar los nuevos datos
        with table.batch_writer() as batch:
            for row in rows:
                batch.put_item(Item=row)

        # ==============================
        #   5. Retornar respuesta estilo Lambda
        # ==============================
        return {
            'statusCode': 200,
            'body': rows
        }

    finally:
        driver.quit()
