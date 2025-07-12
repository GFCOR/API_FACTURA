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

# Cliente HTTP para llamadas externas (urllib3 viene con boto3)
http = urllib3.PoolManager()

# URLs de las lambdas (configurar como variables de entorno)
USUARIO_LAMBDA_URL = os.environ.get('USUARIO_LAMBDA_URL', 'https://7q0ekap8l8.execute-api.us-east-1.amazonaws.com/dev/usuarios/obtener')
PRODUCTO_LAMBDA_URL = os.environ.get('PRODUCTO_LAMBDA_URL', 'https://3hy80u5ihe.execute-api.us-east-1.amazonaws.com/dev')

def obtener_datos_usuario(usuario_id, tenant_id):
    """Obtiene datos del usuario desde otra función Lambda"""
    logger.info(f"🔍 PROCESO: Iniciando obtención de datos del usuario {usuario_id} para tenant {tenant_id}")
    
    try:
        data = {
            'tenant_id': tenant_id,
            'id': usuario_id
        }
        
        logger.info(f"📤 ENVIANDO: Request a servicio usuarios - URL: {USUARIO_LAMBDA_URL}")
        logger.info(f"📤 PAYLOAD: {json.dumps(data)}")
        
        encoded_data = json.dumps(data).encode('utf-8')
        
        response = http.request(
            'POST',
            USUARIO_LAMBDA_URL,
            body=encoded_data,
            headers={'Content-Type': 'application/json'},
            timeout=30.0
        )
        
        logger.info(f"📥 RESPUESTA: HTTP Status {response.status} del servicio usuarios")
        
        if response.status == 200:
            user_data = json.loads(response.data.decode('utf-8'))
            logger.info(f"✅ ÉXITO: Usuario obtenido correctamente - Nombre: {user_data.get('nombres', 'N/A')} {user_data.get('apellidos', 'N/A')}")
            return user_data
        else:
            logger.warning(f"⚠️ ERROR HTTP: Servicio usuarios respondió con status {response.status}")
            logger.warning(f"⚠️ RESPUESTA COMPLETA: {response.data.decode('utf-8')}")
            return None
            
    except urllib3.exceptions.TimeoutError:
        logger.error(f"⏰ TIMEOUT: El servicio de usuarios no respondió en 30 segundos")
        return None
    except urllib3.exceptions.MaxRetryError:
        logger.error(f"🔌 CONEXIÓN: No se pudo conectar al servicio de usuarios")
        return None
    except Exception as e:
        logger.error(f"💥 EXCEPCIÓN: Error inesperado en servicio usuarios - {str(e)}")
        return None

def obtener_datos_producto(producto_id, tenant_id):
    """Obtiene datos del producto desde otra función Lambda"""
    logger.info(f"🔍 PROCESO: Iniciando obtención de datos del producto {producto_id} para tenant {tenant_id}")
    
    try:
        data = {
            'tenant_id': tenant_id,
            'id_producto': producto_id
        }
        
        logger.info(f"📤 ENVIANDO: Request a servicio productos - URL: {PRODUCTO_LAMBDA_URL}")
        logger.info(f"📤 PAYLOAD: {json.dumps(data)}")
        
        encoded_data = json.dumps(data).encode('utf-8')
        
        response = http.request(
            'POST',
            PRODUCTO_LAMBDA_URL,
            body=encoded_data,
            headers={'Content-Type': 'application/json'},
            timeout=30.0
        )
        
        logger.info(f"📥 RESPUESTA: HTTP Status {response.status} del servicio productos")
        
        if response.status == 200:
            product_data = json.loads(response.data.decode('utf-8'))
            logger.info(f"✅ ÉXITO: Producto obtenido correctamente - Nombre: {product_data.get('nombre', 'N/A')}, Precio: {product_data.get('precio', 'N/A')}")
            return product_data
        else:
            logger.warning(f"⚠️ ERROR HTTP: Servicio productos respondió con status {response.status}")
            logger.warning(f"⚠️ RESPUESTA COMPLETA: {response.data.decode('utf-8')}")
            return None
            
    except urllib3.exceptions.TimeoutError:
        logger.error(f"⏰ TIMEOUT: El servicio de productos no respondió en 30 segundos")
        return None
    except urllib3.exceptions.MaxRetryError:
        logger.error(f"🔌 CONEXIÓN: No se pudo conectar al servicio de productos")
        return None
    except Exception as e:
        logger.error(f"💥 EXCEPCIÓN: Error inesperado en servicio productos - {str(e)}")
        return None

def crear_factura(factura_data, tenant_id):
    """Crea una nueva factura en DynamoDB"""
    logger.info(f"🔍 PROCESO: Iniciando creación de factura en DynamoDB para tenant {tenant_id}")
    
    try:
        factura_id = str(uuid.uuid4())
        logger.info(f"🆔 GENERADO: Nuevo ID de factura - {factura_id}")
        
        # Convertir floats a Decimal para DynamoDB
        logger.info(f"🔄 PROCESO: Convirtiendo datos a formato DynamoDB")
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
        
        logger.info(f"💾 GUARDANDO: Insertando factura en DynamoDB - Total: ${factura_data['total']}")
        table.put_item(Item=item)
        
        logger.info(f"✅ ÉXITO: Factura guardada exitosamente en DynamoDB")
        return item  # Retornar el objeto completo en lugar de solo el ID
    except Exception as e:
        logger.error(f"💥 ERROR DYNAMODB: Fallo al guardar en base de datos - {str(e)}")
        raise Exception(f"Error al crear factura en base de datos: {str(e)}")

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
    logger.info(f"🚀 INICIO: Lambda CrearFactura iniciado - Request ID: {context.aws_request_id}")
    
    try:
        # DEBUGGING: Log completo del evento
        logger.info(f"📋 DEBUG: Evento completo recibido")
        logger.debug(f"📋 EVENTO RAW: {json.dumps(event)}")
        
        # PROCESO 1: Parsear el body del request
        logger.info(f"🔍 PROCESO 1: Parseando body del request")
        
        # Manejar tanto invocación directa como a través de API Gateway
        if 'body' in event and event['body'] is not None:
            # Caso API Gateway (con proxy integration)
            try:
                # Limpiar el body de caracteres problemáticos y espacios extra
                raw_body = event['body']
                logger.info(f"📋 RAW BODY LENGTH: {len(raw_body)} caracteres")
                logger.info(f"📋 RAW BODY PREVIEW: {raw_body[:200]}...")
                
                # Limpiar caracteres de control y normalizar
                cleaned_body = raw_body.strip().replace('\r\n', '\n').replace('\r', '\n')
                logger.info(f"📋 CLEANED BODY LENGTH: {len(cleaned_body)} caracteres")
                
                body = json.loads(cleaned_body)
                logger.info(f"✅ ÉXITO 1: Body parseado correctamente desde API Gateway")
                logger.info(f"📊 DATOS: {json.dumps(body)}")
            except json.JSONDecodeError as e:
                logger.error(f"💥 ERROR 1: No se pudo parsear el body como JSON - {str(e)}")
                logger.error(f"💥 ERROR POSITION: Línea {e.lineno}, Columna {e.colno}, Carácter {e.pos}")
                
                # Mostrar el contexto alrededor del error
                try:
                    error_context = event['body'][max(0, e.pos-50):e.pos+50]
                    logger.error(f"💥 ERROR CONTEXT: ...{error_context}...")
                except:
                    pass
                
                return {
                    'statusCode': 400,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': 'PROCESO 1 FALLÓ: El body del request no es JSON válido',
                        'detalle': f'Error de parseo: {str(e)}',
                        'proceso_fallido': 'Validación de formato JSON',
                        'error_posicion': {
                            'linea': e.lineno,
                            'columna': e.colno,
                            'caracter': e.pos
                        },
                        'raw_body_length': len(event['body']),
                        'raw_body_preview': event['body'][:300] if len(event['body']) > 300 else event['body']
                    }, indent=2, ensure_ascii=False)
                }
            except Exception as e:
                logger.error(f"💥 ERROR 1: Error inesperado al procesar body - {str(e)}")
                return {
                    'statusCode': 400,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': 'PROCESO 1 FALLÓ: Error inesperado al procesar el body',
                        'detalle': f'Error: {str(e)}',
                        'proceso_fallido': 'Procesamiento del body'
                    }, indent=2, ensure_ascii=False)
                }
        else:
            # Caso invocación directa (sin API Gateway)
            body = event
            logger.info(f"✅ ÉXITO 1: Usando evento directo como body")

        # PROCESO 2: Validar campos requeridos
        logger.info(f"🔍 PROCESO 2: Validando campos requeridos")
        
        campos_requeridos = ['tenant_id', 'usuario_id', 'productos']
        for campo in campos_requeridos:
            if campo not in body:
                logger.error(f"💥 ERROR 2: Campo requerido faltante - {campo}")
                return {
                    'statusCode': 400,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': f'PROCESO 2 FALLÓ: Campo requerido faltante - {campo}',
                        'detalle': f'El campo "{campo}" es obligatorio para crear una factura',
                        'proceso_fallido': 'Validación de campos requeridos',
                        'campos_recibidos': list(body.keys()),
                        'campos_requeridos': campos_requeridos
                    }, indent=2, ensure_ascii=False)
                }
        
        logger.info(f"✅ ÉXITO 2: Todos los campos requeridos están presentes")

        # PROCESO 3: Extraer y validar datos
        logger.info(f"🔍 PROCESO 3: Extrayendo datos del request")
        
        tenant_id = body['tenant_id']
        usuario_id = body['usuario_id']
        productos = body['productos']
        
        logger.info(f"📊 DATOS EXTRAÍDOS: Tenant={tenant_id}, Usuario={usuario_id}, Productos={len(productos)}")
        
        if not isinstance(productos, list) or len(productos) == 0:
            logger.error(f"💥 ERROR 3: Lista de productos inválida")
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'PROCESO 3 FALLÓ: Lista de productos inválida',
                    'detalle': 'La lista de productos debe ser un array no vacío',
                    'proceso_fallido': 'Validación de estructura de datos',
                    'productos_recibidos': productos
                }, indent=2, ensure_ascii=False)
            }
        
        logger.info(f"✅ ÉXITO 3: Datos extraídos y validados correctamente")
        
        total = 0.0
        productos_obj = []
        productos_fallidos = []

        # PROCESO 4: Obtener datos del usuario
        logger.info(f"🔍 PROCESO 4: Obteniendo información del usuario")
        
        usuario_info = obtener_datos_usuario(usuario_id, tenant_id)
        if usuario_info is None:
            logger.warning(f"⚠️ ADVERTENCIA 4: No se pudo obtener datos del usuario, usando fallback")
            # Usar datos fallback si el servicio falla
            usuario_info = {
                'id': usuario_id,
                'nombre': 'Usuario no disponible',
                'email': 'no-disponible@temp.com',
                'disponible': False,
                'error': 'Servicio de usuarios no disponible'
            }
        else:
            logger.info(f"✅ ÉXITO 4: Datos del usuario obtenidos correctamente")

        # PROCESO 5: Procesar cada producto
        logger.info(f"🔍 PROCESO 5: Procesando {len(productos)} productos")
        
        for i, producto in enumerate(productos, 1):
            logger.info(f"🔍 PROCESO 5.{i}: Procesando producto {producto.get('id', 'SIN_ID')}")
            
            if 'id' not in producto or 'cantidad' not in producto:
                logger.error(f"💥 ERROR 5.{i}: Producto mal formateado")
                return {
                    'statusCode': 400,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': f'PROCESO 5.{i} FALLÓ: Producto mal formateado',
                        'detalle': 'Cada producto debe tener "id" y "cantidad"',
                        'proceso_fallido': f'Validación de producto #{i}',
                        'producto_problematico': producto
                    }, indent=2, ensure_ascii=False)
                }
            
            producto_info = obtener_datos_producto(producto['id'], tenant_id)
            
            if producto_info is None:
                logger.warning(f"⚠️ ADVERTENCIA 5.{i}: Producto no encontrado, usando fallback")
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
            else:
                logger.info(f"✅ ÉXITO 5.{i}: Producto procesado correctamente")
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

        logger.info(f"✅ ÉXITO 5: Todos los productos procesados - Total calculado: ${total}")
        
        # Log si hubo productos fallidos
        if productos_fallidos:
            logger.warning(f"⚠️ ADVERTENCIA: {len(productos_fallidos)} productos fallidos: {productos_fallidos}")

        # PROCESO 6: Ensamblar datos de la factura
        logger.info(f"🔍 PROCESO 6: Ensamblando datos de la factura")
        
        factura = {
            "usuario_id": usuario_id,
            "productos": productos_obj,
            "total": total,
            "usuario_info": usuario_info,
            "fecha": body.get('fecha', datetime.utcnow().strftime('%Y-%m-%d')),
            "estado": "activa",
            "productos_fallidos": productos_fallidos if productos_fallidos else []
        }
        
        logger.info(f"✅ ÉXITO 6: Datos de factura ensamblados correctamente")

        # PROCESO 7: Guardar en DynamoDB
        logger.info(f"🔍 PROCESO 7: Guardando factura en base de datos")
        
        try:
            factura_creada = crear_factura(factura, tenant_id)
            logger.info(f"✅ ÉXITO 7: Factura guardada exitosamente - ID: {factura_creada['factura_id']}")
        except Exception as e:
            logger.error(f"💥 ERROR 7: Fallo al guardar en base de datos - {str(e)}")
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'PROCESO 7 FALLÓ: Error al guardar en base de datos',
                    'detalle': f'No se pudo guardar la factura en DynamoDB: {str(e)}',
                    'proceso_fallido': 'Guardado en base de datos',
                    'factura_data': {
                        'total': total,
                        'productos_count': len(productos_obj),
                        'tenant_id': tenant_id
                    }
                }, indent=2, ensure_ascii=False)
            }
        
        # PROCESO 8: Respuesta exitosa
        logger.info(f"🎉 PROCESO 8: Generando respuesta exitosa")
        
        logger.info(f"🏁 FINAL: Factura creada completamente - ID: {factura_creada['factura_id']}, Total: ${total}")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(factura_creada, default=decimal_default, indent=2, ensure_ascii=False)
        }

    except Exception as e:
        logger.error(f"💥 ERROR CRÍTICO: Fallo inesperado del sistema - {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'ERROR CRÍTICO DEL SISTEMA',
                'detalle': f'Fallo inesperado no controlado: {str(e)}',
                'proceso_fallido': 'Sistema general',
                'request_id': context.aws_request_id if context else 'N/A'
            }, indent=2, ensure_ascii=False)
        }
