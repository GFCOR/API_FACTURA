
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
        factura_id = body['factura_id']
        # Eliminar la factura
        resultado = eliminar_factura(factura_id, tenant_id)

        if 'error' in resultado:
            if 'no encontrada' in resultado['error']:
                return {
                    'statusCode': 404,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': 'Factura no encontrada',
                        'detalle': 'No existe una factura con el ID y tenant proporcionados.'
                    }, indent=2, ensure_ascii=False)
                }
            else:
                return {
                    'statusCode': 500,
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    },
                    'body': json.dumps({
                        'error': 'Error al eliminar factura',
                        'detalle': resultado['error']
                    }, indent=2, ensure_ascii=False)
                }
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'mensaje': 'Factura eliminada correctamente',
                'factura_id': body['factura_id'],
                'tenant_id': body['tenant_id']
            }, indent=2, ensure_ascii=False)
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