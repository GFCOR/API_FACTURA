import json
import boto3

# Cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Facturas')

def obtener_facturas(tenant_id, skip=0, limit=10, usuario_id=None):
    """Obtiene facturas de DynamoDB con paginación y filtros"""
    try:
        
        if usuario_id:
            # Filtrar por usuario específico
            response = table.query(
                KeyConditionExpression='tenant_id = :tenant_id',
                FilterExpression='usuario_id = :usuario_id',
                ExpressionAttributeValues={
                    ':tenant_id': tenant_id,
                    ':usuario_id': usuario_id
                },
                Limit=limit
            )
        else:
            # Obtener todas las facturas del tenant
            response = table.query(
                KeyConditionExpression='tenant_id = :tenant_id',
                ExpressionAttributeValues={
                    ':tenant_id': tenant_id
                },
                Limit=limit
            )
        
        return response.get('Items', [])
    except Exception as e:
        return {'error': f"Error al obtener facturas: {str(e)}"}

def lambda_handler(event, context):
    try:
        # Parseo robusto del body
        body = event.get('body')
        import re
        import ast
        if isinstance(body, str):
            cleaned_body = body.strip().replace('\r\n', '\n').replace('\r', '\n')
            cleaned_body = re.sub(r'[\x00-\x1F\x7F\u00A0]', '', cleaned_body)
            try:
                body = json.loads(cleaned_body)
            except Exception as e:
                try:
                    body = ast.literal_eval(cleaned_body)
                except Exception as e2:
                    return {
                        'statusCode': 400,
                        'headers': {
                            'Content-Type': 'application/json',
                            'Access-Control-Allow-Origin': '*'
                        },
                        'body': json.dumps({
                            'error': 'El body del request no es JSON válido',
                            'detalle': f'json.loads: {str(e)} | ast.literal_eval: {str(e2)}',
                            'raw_body': cleaned_body
                        }, indent=2, ensure_ascii=False)
                    }
        tenant_id = body['tenant_id']
        skip = body.get('skip', 0)
        limit = body.get('limit', 10)
        usuario_id = body.get('usuario_id', None)  # Opcional para filtrar por usuario
        # Llamar al servicio que obtiene las facturas
        facturas = obtener_facturas(tenant_id, skip=skip, limit=limit, usuario_id=usuario_id)

        if isinstance(facturas, dict) and 'error' in facturas:
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Error al obtener facturas',
                    'detalle': facturas['error']
                }, indent=2, ensure_ascii=False)
            }
        if not facturas:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'No se encontraron facturas',
                    'detalle': 'No existen facturas para los filtros proporcionados.'
                }, indent=2, ensure_ascii=False)
            }
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'mensaje': 'Facturas encontradas correctamente',
                'cantidad': len(facturas),
                'facturas': facturas
            }, indent=2, ensure_ascii=False, default=str)
        }

    except KeyError as e:
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Campo requerido faltante',
                'detalle': f'El campo {str(e)} es obligatorio para listar facturas.'
            }, indent=2, ensure_ascii=False)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Error inesperado al listar facturas',
                'detalle': str(e)
            }, indent=2, ensure_ascii=False)
        }