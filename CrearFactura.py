import json
import boto3
from datetime import datetime
import uuid
import urllib3
import os
import logging
from decimal import Decimal

# --- Configuración Inicial ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    dynamodb_resource = boto3.resource('dynamodb')
    s3_client = boto3.client('s3')
    http = urllib3.PoolManager()
    glue_client = boto3.client('glue') 
    # ## --- NUEVA LÍNEA: Cliente de AWS Lambda para invocación --- ##
    lambda_client = boto3.client('lambda') 
except Exception as e:
    logger.error(f"Error inicializando clientes de AWS: {str(e)}")
    raise e

DYNAMODB_TABLE_NAME = 'facturas-api-dev'
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'pf-facturas-sergio')
USUARIO_LAMBDA_URL = 'https://30ipk5jpl6.execute-api.us-east-1.amazonaws.com/dev/usuarios/obtener'
PRODUCTO_LAMBDA_URL = 'https://1kobbmlfu9.execute-api.us-east-1.amazonaws.com/dev/productos/obtener'
# ## --- NUEVA VARIABLE: Nombre de la Lambda a invocar --- ##
ATHENA_REPAIR_LAMBDA_NAME = os.environ.get('ATHENA_REPAIR_LAMBDA_NAME', 'AthenaRepairTableFacturas')

# --- Funciones de Ayuda (sin cambios) ---
def obtener_datos_externos(url, method='POST', data=None):
    try:
        headers = {'Content-Type': 'application/json'}
        encoded_data = json.dumps(data).encode('utf-8') if data else None
        response = http.request(method, url, body=encoded_data, headers=headers, timeout=10.0)
        logger.info(f"Respuesta de {url}: Status {response.status}")
        if response.status == 200:
            return json.loads(response.data.decode('utf-8'))
        else:
            logger.warning(f"Error en llamada a {url}: Status {response.status}, Body: {response.data.decode('utf-8')}")
            return None
    except Exception as e:
        logger.error(f"Excepción al llamar a {url}: {str(e)}")
        return None

def convert_floats_to_decimals(obj):
    if isinstance(obj, float): return Decimal(str(obj))
    if isinstance(obj, dict): return {k: convert_floats_to_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list): return [convert_floats_to_decimals(item) for item in obj]
    return obj

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)

def add_partition_to_glue(tenant_id, fecha, bucket_name, table_name="pf_facturas_sergio", database_name="facturas_db"):
    try:
        partition_location = f"s3://{bucket_name}/{tenant_id}/facturas/{fecha}/"
        partition_values = [tenant_id, fecha] 
        try:
            glue_client.get_partition(
                DatabaseName=database_name, TableName=table_name, PartitionValues=partition_values
            )
            logger.info(f"Partición {partition_values} ya existe en Glue para {table_name}. No se hace nada.")
        except glue_client.exceptions.EntityNotFoundException:
            glue_client.create_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionInput={'Values': partition_values, 'StorageDescriptor': {'Location': partition_location, 'SerdeInfo': {'SerializationLibrary': 'org.openx.data.jsonserde.JsonSerDe', 'Parameters': {'ignore.malformed.json': 'true'}}, 'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat', 'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'}}
            )
            logger.info(f"Partición {partition_values} creada exitosamente en Glue para {table_name}.")
    except Exception as e:
        logger.error(f"Error al añadir/verificar partición en Glue para {tenant_id}/{fecha}: {str(e)}", exc_info=True)


# --- Handler Principal de la Lambda ---
def lambda_handler(event, context):
    logger.info(f"Iniciando lambda 'crear_factura_completa'. Request ID: {context.aws_request_id}")

    try:
        # --- 1. Parsear y Validar Input ---
        body = json.loads(event.get('body', '{}'))
        tenant_id = body.get('tenant_id')
        usuario_id = body.get('usuario_id')
        productos_req = body.get('productos')

        if not all([tenant_id, usuario_id, productos_req]):
            return {
                "statusCode": 400,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"error": "Faltan campos: 'tenant_id', 'usuario_id', 'productos'."})
            }
        
        # --- 2. Enriquecer y Validar Datos Estrictamente ---
        logger.info("Paso 2: Enriqueciendo y validando datos desde servicios externos.")
        
        usuario_info_respuesta = obtener_datos_externos(USUARIO_LAMBDA_URL, data={'tenant_id': tenant_id, 'id': usuario_id})

        if not (usuario_info_respuesta and 'user' in usuario_info_respuesta):
            error_msg = f"Usuario con ID '{usuario_id}' no encontrado para el tenant '{tenant_id}'."
            logger.error(error_msg)
            return {
                "statusCode": 404,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"error": error_msg})
            }
        
        usuario_info = usuario_info_respuesta['user']
        logger.info(f"Usuario {usuario_id} encontrado: {usuario_info.get('nombres')}")

        if 'direccion' in usuario_info and isinstance(usuario_info['direccion'], str):
            try:
                logger.info(f"Detectado campo 'direccion' como string. Intentando deserializar: {usuario_info['direccion']}")
                usuario_info['direccion'] = json.loads(usuario_info['direccion'])
                logger.info("El campo 'direccion' ha sido deserializado a un objeto struct correctamente.")
            except json.JSONDecodeError:
                logger.warning("El campo 'direccion' no era un JSON válido. Se establecerá como nulo.")
                usuario_info['direccion'] = None
        
        total_factura = Decimal('0.0')
        productos_procesados = []

        for prod_req in productos_req:
            prod_id = prod_req.get('id')
            cantidad = prod_req.get('cantidad', 1)
            
            logger.info(f"Obteniendo datos para producto: {prod_id}")
            producto_info_respuesta = obtener_datos_externos(f"{PRODUCTO_LAMBDA_URL}?tenant_id={tenant_id}&id_producto={prod_id}", method='GET')
            
            if not (producto_info_respuesta and 'product' in producto_info_respuesta):
                error_msg = f"Producto con ID '{prod_id}' no encontrado para el tenant '{tenant_id}'."
                logger.error(error_msg)
                return {
                    "statusCode": 404,
                    "headers": {
                        "Content-Type": "application/json",
                        "Access-Control-Allow-Origin": "*"
                    },
                    "body": json.dumps({"error": error_msg})
                }

            producto_real = producto_info_respuesta['product']
            logger.info(f"Producto {prod_id} encontrado: {producto_real.get('nombre')}")
            
            precio_str = producto_real.get('precio', '0')
            precio_unitario = Decimal(precio_str)
            subtotal = precio_unitario * Decimal(cantidad)
            total_factura += subtotal
            
            productos_procesados.append({
                'id_prod': prod_id,
                'nombre': producto_real.get('nombre', 'Producto sin nombre'),
                'precio_unitario': precio_unitario,
                'cantidad': cantidad,
                'subtotal': subtotal
            })

        # --- 3. Ensamblar el Objeto Final de la Factura ---
        logger.info("Paso 3: Ensamblando objeto final de la factura.")
        
        factura_id = str(uuid.uuid4())
        fecha_actual = datetime.utcnow()

        factura_final = {
            'factura_id': factura_id,
            'tenant_id': tenant_id,
            'fecha': fecha_actual.strftime('%Y-%m-%d'),
            'fecha_creacion': fecha_actual.isoformat(),
            'usuario_info': usuario_info,
            'productos': productos_procesados,
            'total': total_factura,
            'estado': 'activa',
            'productos_fallidos': []
        }
        
        factura_dynamodb = convert_floats_to_decimals(factura_final)
        
        # --- 4. Guardar en DynamoDB ---
        logger.info(f"Paso 4: Guardando factura {factura_id} en DynamoDB.")
        table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
        table.put_item(Item=factura_dynamodb)
        logger.info("Guardado en DynamoDB exitoso.")

        # --- 5. Archivando en S3 ---
        logger.info(f"Paso 5: Archivando factura {factura_id} en S3.")
        s3_key = f"{tenant_id}/facturas/{factura_final['fecha']}/{factura_id}.json"
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=json.dumps(factura_final, cls=DecimalEncoder, ensure_ascii=False), ContentType="application/json")
        logger.info(f"Archivado en S3 exitoso en la ruta: s3://{S3_BUCKET_NAME}/{s3_key}")

        add_partition_to_glue(tenant_id, factura_final['fecha'], S3_BUCKET_NAME) 

        # ## --- NUEVA LÓGICA: Invocar la Lambda de reparación de Athena --- ##
        try:
            if ATHENA_REPAIR_LAMBDA_NAME:
                lambda_client.invoke(
                    FunctionName=ATHENA_REPAIR_LAMBDA_NAME,
                    InvocationType='Event',
                    Payload=json.dumps({"detail": "new_invoice_created", "factura_id": factura_id})
                )
                logger.info(f"Lambda {ATHENA_REPAIR_LAMBDA_NAME} invocada exitosamente de forma asíncrona para factura {factura_id}.")
            else:
                logger.warning("ATHENA_REPAIR_LAMBDA_NAME no está configurada. No se invocará la Lambda de reparación.")
        except Exception as e:
            logger.error(f"Error al invocar la Lambda {ATHENA_REPAIR_LAMBDA_NAME}: {str(e)}", exc_info=True)

        logger.info("Proceso completado.")
        return {
            'statusCode': 201,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'mensaje': 'Factura creada, enriquecida y archivada exitosamente', 'factura': factura_final}, cls=DecimalEncoder, indent=2)
        }

    except json.JSONDecodeError as e:
        logger.error(f"Error de parseo JSON: {str(e)}")
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": "Cuerpo de la petición no es un JSON válido."})
        }
    except Exception as e:
        logger.error(f"Error inesperado durante la ejecución: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": "Ocurrió un error interno en el servidor."})
        }