import json
import boto3
from datetime import datetime
import uuid
import urllib3
import os
import logging
from decimal import Decimal

# --- Configuración Inicial ---

# 1. Configurar logging detallado para CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 2. Inicializar clientes de AWS (se reutilizarán)
try:
    dynamodb_resource = boto3.resource('dynamodb')
    s3_client = boto3.client('s3')
    http = urllib3.PoolManager()
except Exception as e:
    logger.error(f"Error inicializando clientes de AWS: {str(e)}")
    raise e

# 3. Obtener configuración
#    (Recomendación: Mueve estas URLs a variables de entorno para más flexibilidad)
DYNAMODB_TABLE_NAME = 'facturas-api-dev'
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'pf-facturas-sergio')
USUARIO_LAMBDA_URL = 'https://30ipk5jpl6.execute-api.us-east-1.amazonaws.com/dev/usuarios/obtener'
PRODUCTO_LAMBDA_URL = 'https://1kobbmlfu9.execute-api.us-east-1.amazonaws.com/dev/productos/obtener'

# --- Funciones de Ayuda ---
# (Estas funciones están bien, no necesitan cambios)

def obtener_datos_externos(url, method='POST', data=None):
    """Función genérica para llamar a otras Lambdas/APIs."""
    try:
        headers = {'Content-Type': 'application/json'}
        encoded_data = json.dumps(data).encode('utf-8') if data else None
        response = http.request(method, url, body=encoded_data, headers=headers, timeout=10.0)
        logger.info(f"Respuesta de {url}: Status {response.status}")
        if response.status == 200:
            return json.loads(response.data.decode('utf-8'))
        else:
            logger.warning(f"Error en llamada a {url}: {response.data.decode('utf-8')}")
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

# --- Handler Principal de la Lambda ---

def lambda_handler(event, context):
    logger.info(f"Iniciando lambda 'crear_factura_completa'. Request ID: {context.aws_request_id}")

    try:
        # --- 1. Parsear y Validar Input ---
        logger.info("Paso 1: Parseando y validando input.")
        body = json.loads(event.get('body', '{}'))
        tenant_id = body.get('tenant_id')
        usuario_id = body.get('usuario_id')
        productos_req = body.get('productos')

        if not all([tenant_id, usuario_id, productos_req]):
            return {"statusCode": 400, "body": json.dumps({"error": "Faltan campos: 'tenant_id', 'usuario_id', 'productos'."})}
        
        # --- 2. Enriquecer Datos (Llamadas a otros servicios) ---
        logger.info("Paso 2: Enriqueciendo datos desde servicios externos.")
        
        # ## --- CORRECCIÓN DE ESQUEMA PARA USUARIO_INFO --- ##
        usuario_info_respuesta = obtener_datos_externos(USUARIO_LAMBDA_URL, data={'tenant_id': tenant_id, 'id': usuario_id})

        if usuario_info_respuesta and 'user' in usuario_info_respuesta:
            usuario_info = usuario_info_respuesta['user']
            # Añadimos un campo para saber si la información es completa
            usuario_info['error'] = False 
            logger.info(f"Usuario {usuario_id} encontrado: {usuario_info.get('nombres')}")
        else:
            logger.warning(f"No se pudo obtener información para el usuario {usuario_id}. Usando fallback con esquema consistente.")
            # Creamos un objeto con la MISMA ESTRUCTURA que un usuario exitoso
            usuario_info = {
                'tenant_id': tenant_id,
                'telefono': None,
                'fecha_registro': None,
                'apellidos': None,
                'email': None,
                'rol': None,
                'id': usuario_id,
                'direccion': None,
                'nombres': 'Usuario no disponible',
                'error': True # Campo para indicar que los datos son de fallback
            }
        
        # El resto del código no necesita cambios, ya que el manejo de productos era consistente.
        
        # Procesar productos y calcular total
        total_factura = Decimal('0.0')
        productos_procesados = []
        productos_fallidos = []

        for prod_req in productos_req:
            prod_id = prod_req.get('id')
            cantidad = prod_req.get('cantidad', 1)
            
            logger.info(f"Obteniendo datos para producto: {prod_id}")
            producto_info_respuesta = obtener_datos_externos(f"{PRODUCTO_LAMBDA_URL}?tenant_id={tenant_id}&id_producto={prod_id}", method='GET')
            
            if producto_info_respuesta and 'product' in producto_info_respuesta:
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
            else:
                logger.warning(f"No se pudo obtener información para el producto {prod_id}. Respuesta recibida: {producto_info_respuesta}")
                productos_fallidos.append(prod_id)

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
            'productos_fallidos': productos_fallidos
        }
        
        factura_dynamodb = convert_floats_to_decimals(factura_final)
        
        # --- 4. Guardar en DynamoDB ---
        logger.info(f"Paso 4: Guardando factura {factura_id} en DynamoDB.")
        table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
        table.put_item(Item=factura_dynamodb)
        logger.info("Guardado en DynamoDB exitoso.")

        # --- 5. Archivar en S3 ---
        logger.info(f"Paso 5: Archivando factura {factura_id} en S3.")
        s3_key = f"{tenant_id}/facturas/{factura_final['fecha']}/{factura_id}.json"
        
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(factura_final, cls=DecimalEncoder, indent=2, ensure_ascii=False),
            ContentType="application/json"
        )
        logger.info(f"Archivado en S3 exitoso en la ruta: s3://{S3_BUCKET_NAME}/{s3_key}")

        # --- 6. Devolver Respuesta Exitosa ---
        logger.info("Proceso completado.")
        return {
            'statusCode': 201,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'mensaje': 'Factura creada, enriquecida y archivada exitosamente', 'factura': factura_final}, cls=DecimalEncoder, indent=2)
        }

    except json.JSONDecodeError as e:
        logger.error(f"Error de parseo JSON: {str(e)}")
        return {"statusCode": 400, "body": json.dumps({"error": "Cuerpo de la petición no es un JSON válido."})}
    except Exception as e:
        logger.error(f"Error inesperado durante la ejecución: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": "Ocurrió un error interno en el servidor."})}