import json
import boto3
from datetime import datetime

# Cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Facturas')

def actualizar_factura(factura_id, compra_modificada, tenant_id):
    """Actualiza una factura existente"""
    try:
        
        # Verificar que la factura existe
        response = table.get_item(
            Key={
                'tenant_id': tenant_id,
                'factura_id': factura_id
            }
        )
        
        if 'Item' not in response:
            return {'error': 'Factura no encontrada'}
        
        # Actualizar la factura
        update_expression = "SET productos = :productos, total = :total, fecha_actualizacion = :fecha_act"
        expression_values = {
            ':productos': compra_modificada.get('productos', []),
            ':total': compra_modificada.get('total', 0),
            ':fecha_act': datetime.utcnow().isoformat()
        }
        
        table.update_item(
            Key={
                'tenant_id': tenant_id,
                'factura_id': factura_id
            },
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values
        )
        
        return {'success': True}
    except Exception as e:
        return {'error': f"Error al actualizar factura: {str(e)}"}

def lambda_handler(event, context):
    try:
        # Obtener datos del body
        body = json.loads(event['body'])
        tenant_id = body['tenant_id']
        factura_id = body['factura_id']
        compra_modificada = body['compra']

        # Llamar al servicio para actualizar la factura
        resultado = actualizar_factura(factura_id, compra_modificada, tenant_id)

        if 'error' in resultado:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': resultado['error']})
            }

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Factura actualizada correctamente'})
        }

    except KeyError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': f'Campo requerido faltante: {str(e)}'})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Error al actualizar la factura: {str(e)}"})
        }