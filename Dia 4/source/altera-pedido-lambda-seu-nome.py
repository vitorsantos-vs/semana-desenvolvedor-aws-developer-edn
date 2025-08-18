import json
import os
import boto3
from datetime import datetime

DYNAMODB_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

def lambda_handler(event, context):
    print(f"Evento SQS (alteraÃ§Ã£o) recebido: {event}")
    for record in event['Records']:
        pedido_id = None
        try:
            eventbridge_event = json.loads(record['body'])
            print(f"Evento EventBridge recebido via SQS: {eventbridge_event}")
            if 'detail' not in eventbridge_event:
                print(f"Erro: Campo 'detail' nÃ£o encontrado no evento: {eventbridge_event}")
                continue

            pedido_data = eventbridge_event['detail']
            pedido_id = pedido_data.get('pedidoId')
            # Exemplo: Espera-se que o evento de alteraÃ§Ã£o contenha os novos itens
            novos_itens = pedido_data.get('novosItens')

            if not pedido_id or novos_itens is None: # Checa se novosItens estÃ¡ presente, mesmo que seja lista vazia
                print(f"Erro: pedidoId ou novosItens nÃ£o encontrados nos detalhes do evento: {pedido_data}")
                continue

            print(f"Processando alteraÃ§Ã£o para pedido: {pedido_id} com novos itens: {novos_itens}")

            # Atualizar itens e status no DynamoDB
            response = table.update_item(
                Key={'pedidoId': str(pedido_id)},
                UpdateExpression="SET itens = :i, statusPedido = :s, timestampAtualizacao = :ts",
                ExpressionAttributeValues={
                    ':i': novos_itens, # Assume que novosItens jÃ¡ estÃ¡ no formato correto
                    ':s': 'ALTERADO',
                    ':ts': datetime.utcnow().isoformat() + "Z"
                },
                ReturnValues="UPDATED_NEW"
            )
            print(f"Pedido {pedido_id} atualizado para ALTERADO. Resposta DynamoDB: {response}")

        except json.JSONDecodeError as je:
            print(f"Erro de JSON ao processar registro {record['messageId']}: {str(je)}")
            raise je
        except Exception as e:
            print(f"Erro geral ao processar alteraÃ§Ã£o {record['messageId']} (pedidoId: {pedido_id}): {str(e)}")
            raise e
    return {'statusCode': 200, 'body': 'Processamento de alteraÃ§Ãµes concluÃ­do'}
