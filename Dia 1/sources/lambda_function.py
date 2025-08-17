import json
import os
import boto3

SQS_QUEUE_URL = os.environ['SQS_QUEUE_URL']
sqs = boto3.client('sqs')

def lambda_handler(event, context):
    print(f"Evento recebido do API Gateway: {event}")
    try:
        body_str = event.get('body', '{}')
        if not body_str:
            body_str = '{}'
        body = json.loads(body_str)

        pedido_id = body.get('pedidoId')
        cliente_id = body.get('clienteId')

        if not pedido_id or not cliente_id:
            print("Erro: pedidoId ou clienteId ausente.")
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'message': 'pedidoId e clienteId sÃ£o obrigatÃ³rios'})
            }

        response = sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(body),
            MessageGroupId=str(pedido_id),
            MessageDeduplicationId=str(context.aws_request_id)
        )
        print(f"Mensagem enviada para SQS: {response['MessageId']}")
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'message': 'Pedido recebido e enfileirado', 'sqsMessageId': response['MessageId']})
        }
    except json.JSONDecodeError:
        print("Erro: Corpo da requisiÃ§Ã£o nÃ£o Ã© um JSON vÃ¡lido.")
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'message': 'Corpo da requisiÃ§Ã£o invÃ¡lido, nÃ£o Ã© um JSON vÃ¡lido'})
        }
    except Exception as e:
        print(f"Erro inesperado: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'message': f'Erro interno do servidor: {str(e)}'})
        }
