import json
import boto3

# Cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Facturas')

def obtener_factura_por_id(factura_id, tenant_id):
    """Obtiene una factura específica por ID"""
    try:
        
        response = table.get_item(
            Key={
                'tenant_id': tenant_id,
                'factura_id': factura_id
            }
        )
        
        if 'Item' not in response:
            return {'error': 'Factura no encontrada'}
        
        return response['Item']
    except Exception as e:
        return {'error': f"Error al obtener factura: {str(e)}"}

def lambda_handler(event, context):
    try:
        # Obtener datos del body
        body = json.loads(event['body'])
        tenant_id = body['tenant_id']
        factura_id = body['factura_id']
        
        # Obtener la factura por ID
        factura = obtener_factura_por_id(factura_id, tenant_id)
        
        if "error" in factura:
            return {
                'statusCode': 404,
                'body': json.dumps(factura)
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps(factura)
        }

    except KeyError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': f'Campo requerido faltante: {str(e)}'})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Error al obtener la factura: {str(e)}"})
        }