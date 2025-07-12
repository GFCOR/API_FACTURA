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
        # Obtener datos del body
        body = json.loads(event['body'])
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
                'body': json.dumps(facturas, indent=2, ensure_ascii=False)
            }

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(facturas, indent=2, ensure_ascii=False, default=str)
        }

    except KeyError as e:
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': f'Campo requerido faltante: {str(e)}'}, indent=2, ensure_ascii=False)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': f"Error al listar las facturas: {str(e)}"}, indent=2, ensure_ascii=False)
        }