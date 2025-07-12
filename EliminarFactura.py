
import json
import boto3

# Cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Facturas')

def eliminar_factura(factura_id, tenant_id):
    """Elimina una factura específica"""
    try:
        
        # Verificar que la factura existe antes de eliminar
        response = table.get_item(
            Key={
                'tenant_id': tenant_id,
                'factura_id': factura_id
            }
        )
        
        if 'Item' not in response:
            return {'error': 'Factura no encontrada'}
        
        # Eliminar la factura
        table.delete_item(
            Key={
                'tenant_id': tenant_id,
                'factura_id': factura_id
            }
        )
        
        return {'success': True}
    except Exception as e:
        return {'error': f"Error al eliminar factura: {str(e)}"}

def lambda_handler(event, context):
    try:
        # Obtener datos del body
        body = json.loads(event['body'])
        tenant_id = body['tenant_id']
        factura_id = body['factura_id']
        
        # Eliminar la factura
        resultado = eliminar_factura(factura_id, tenant_id)

        if 'error' in resultado:
            return {
                'statusCode': 404 if 'no encontrada' in resultado['error'] else 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': resultado['error']}, indent=2, ensure_ascii=False)
            }

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'message': 'Factura eliminada correctamente'}, indent=2, ensure_ascii=False)
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
            'body': json.dumps({'error': f"Error al eliminar la factura: {str(e)}"}, indent=2, ensure_ascii=False)
        }