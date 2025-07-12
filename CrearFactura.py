import json
import boto3
from datetime import datetime
import uuid
import urllib3
import os
import logging
from decimal import Decimal

# Configurar logging para CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Facturas')

# Cliente HTTP para llamadas a otras lambdas
http = urllib3.PoolManager()

# URLs de las lambdas (configurar como variables de entorno)
USUARIO_LAMBDA_URL = os.environ.get('USUARIO_LAMBDA_URL', 'https://7q0ekap8l8.execute-api.us-east-1.amazonaws.com/dev/usuarios/obtener')
PRODUCTO_LAMBDA_URL = os.environ.get('PRODUCTO_LAMBDA_URL', 'https://3hy80u5ihe.execute-api.us-east-1.amazonaws.com/dev')

def obtener_datos_usuario(usuario_id, tenant_id):
    """Obtiene datos del usuario desde otra función Lambda"""
    try:
        body = json.dumps({
            'tenant_id': tenant_id,
            'id': usuario_id
        })
        
        response = http.request(
            'POST',
            USUARIO_LAMBDA_URL,
            body=body,
            headers={
                'Content-Type': 'application/json'
            }
        )
        
        if response.status == 200:
            data = json.loads(response.data.decode('utf-8'))
            return data
        else:
            # Log de alerta para CloudWatch
            logger.warning(f"ALERTA: Error al obtener usuario {usuario_id} del tenant {tenant_id}. HTTP Status: {response.status}")
            return None
            
    except Exception as e:
        # Log de alerta para CloudWatch
        logger.warning(f"ALERTA: Fallo conexión servicio usuarios. Usuario: {usuario_id}, Tenant: {tenant_id}, Error: {str(e)}")
        return None

def obtener_datos_producto(producto_id, tenant_id):
    """Obtiene datos del producto desde otra función Lambda"""
    try:
        body = json.dumps({
            'tenant_id': tenant_id,
            'id_producto': producto_id
        })
        
        response = http.request(
            'POST',
            PRODUCTO_LAMBDA_URL,
            body=body,
            headers={
                'Content-Type': 'application/json'
            }
        )
        
        if response.status == 200:
            data = json.loads(response.data.decode('utf-8'))
            return data
        else:
            # Log de alerta para CloudWatch
            logger.warning(f"ALERTA: Error al obtener producto {producto_id} del tenant {tenant_id}. HTTP Status: {response.status}")
            return None
            
    except Exception as e:
        # Log de alerta para CloudWatch
        logger.warning(f"ALERTA: Fallo conexión servicio productos. Producto: {producto_id}, Tenant: {tenant_id}, Error: {str(e)}")
        return None

def crear_factura(factura_data, tenant_id):
    """Crea una nueva factura en DynamoDB"""
    try:
        factura_id = str(uuid.uuid4())
        
        # Convertir floats a Decimal para DynamoDB
        item = {
            'tenant_id': tenant_id,
            'factura_id': factura_id,
            'usuario_id': factura_data['usuario_id'],
            'productos': convert_floats_to_decimals(factura_data['productos']),
            'total': Decimal(str(factura_data['total'])),
            'usuario_info': factura_data['usuario_info'],
            'fecha': factura_data['fecha'],
            'fecha_creacion': datetime.utcnow().isoformat(),
            'estado': factura_data.get('estado', 'activa')
        }
        
        table.put_item(Item=item)
        return item  # Retornar el objeto completo en lugar de solo el ID
    except Exception as e:
        raise Exception(f"Error al crear factura: {str(e)}")

def convert_floats_to_decimals(obj):
    """Convierte recursivamente floats a Decimals para DynamoDB"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimals(item) for item in obj]
    else:
        return obj

def decimal_default(obj):
    """Función para serializar Decimals a JSON"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def lambda_handler(event, context):
    try:
        # DEBUGGING: Log completo del evento
        logger.info(f"DEBUG: Evento completo recibido: {json.dumps(event)}")
        
        # Manejar tanto invocación directa como a través de API Gateway
        if 'body' in event and event['body'] is not None:
            # Caso API Gateway (con proxy integration)
            try:
                body = json.loads(event['body'])
                logger.info(f"DEBUG: Invocación vía API Gateway - Body parseado: {json.dumps(body)}")
            except json.JSONDecodeError as e:
                logger.error(f"ERROR: No se pudo parsear el body como JSON: {str(e)}")
                return {
                    'statusCode': 400,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': 'Body no es JSON válido',
                        'raw_body': event['body']
                    }, indent=2, ensure_ascii=False)
                }
        else:
            # Caso invocación directa (sin API Gateway)
            body = event
            logger.info(f"DEBUG: Invocación directa - Usando evento como body: {json.dumps(body)}")

        # Verificar campos requeridos
        campos_requeridos = ['tenant_id', 'usuario_id', 'productos']
        for campo in campos_requeridos:
            if campo not in body:
                logger.error(f"ERROR: Campo requerido faltante: {campo}")
                return {
                    'statusCode': 400,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': f'Campo requerido faltante: {campo}',
                        'body_keys': list(body.keys())
                    }, indent=2, ensure_ascii=False)
                }

        # Continuar con el procesamiento normal
        tenant_id = body['tenant_id']
        usuario_id = body['usuario_id']
        productos = body['productos']
        total = 0.0
        productos_obj = []
        productos_fallidos = []

        # Obtener datos del usuario
        usuario_info = obtener_datos_usuario(usuario_id, tenant_id)
        if usuario_info is None:
            # Usar datos fallback si el servicio falla
            usuario_info = {
                'id': usuario_id,
                'nombre': 'Usuario no disponible',
                'email': 'no-disponible@temp.com',
                'disponible': False
            }
            logger.info(f"INFO: Usando datos fallback para usuario {usuario_id}")

        # Obtener datos de cada producto y calcular el total
        for producto in productos:
            producto_info = obtener_datos_producto(producto['id'], tenant_id)
            
            if producto_info is None:
                # Agregar producto fallido a la lista pero continuar
                productos_fallidos.append(producto['id'])
                producto_obj_fallback = {
                    'id_prod': producto['id'],
                    'precio_unitario': Decimal('0.0'),
                    'cantidad': int(producto['cantidad']),
                    'subtotal': Decimal('0.0'),
                    'nombre': 'Producto no disponible',
                    'disponible': False
                }
                productos_obj.append(producto_obj_fallback)
                logger.info(f"INFO: Usando datos fallback para producto {producto['id']}")
            else:
                # Producto obtenido correctamente
                precio_unitario = Decimal(str(producto_info.get('precio', 0)))
                cantidad = int(producto['cantidad'])
                subtotal = precio_unitario * cantidad
                total += float(subtotal)  # Sumar como float para el cálculo
                
                producto_obj = {
                    'id_prod': producto_info.get('id', producto['id']),
                    'precio_unitario': precio_unitario,
                    'cantidad': cantidad,
                    'subtotal': subtotal,
                    'nombre': producto_info.get('nombre', 'Producto'),
                    'disponible': True
                }
                productos_obj.append(producto_obj)

        # Log si hubo productos fallidos
        if productos_fallidos:
            logger.warning(f"ALERTA: Productos fallidos en factura: {productos_fallidos}")

        # Crear la factura
        factura = {
            "usuario_id": usuario_id,
            "productos": productos_obj,
            "total": total,
            "usuario_info": usuario_info,
            "fecha": body.get('fecha', datetime.utcnow().strftime('%Y-%m-%d')),
            "estado": "activa",
            "productos_fallidos": productos_fallidos if productos_fallidos else []
        }

        factura_creada = crear_factura(factura, tenant_id)
        
        # Log exitoso
        logger.info(f"INFO: Factura creada exitosamente. ID: {factura_creada['factura_id']}")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(factura_creada, default=decimal_default, indent=2, ensure_ascii=False)
        }

    except Exception as e:
        logger.error(f"ERROR: Fallo crítico al crear factura: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': f"Error inesperado: {str(e)}"}, indent=2, ensure_ascii=False)
        }
