import json
import os
import boto3
from datetime import datetime
import urllib.parse
import uuid # Para gerar MessageDeduplicationId se necessÃ¡rio

DYNAMODB_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
SQS_FIFO_PEDIDOS_URL = os.environ['SQS_FIFO_PEDIDOS_URL'] # URL da fila da Aula 1

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
sqs = boto3.client('sqs') # Cliente SQS para enviar para a fila FIFO
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

def lambda_handler(event, context):
    print(f"Evento SQS (arquivos) recebido: {event}")
    for record in event['Records']:
        s3_event_message_body = record['body']
        object_key = None # Para garantir que object_key seja definido
        bucket_name = None  # Para garantir que bucket_name seja definido
        
        try:
            s3_event_message = json.loads(s3_event_message_body)
            if 'Message' in s3_event_message and isinstance(s3_event_message['Message'], str):
                s3_notification = json.loads(s3_event_message['Message'])
            else:
                s3_notification = s3_event_message

            for s3_record in s3_notification['Records']:
                bucket_name = s3_record['s3']['bucket']['name']
                object_key_encoded = s3_record['s3']['object']['key']
                object_key = urllib.parse.unquote_plus(object_key_encoded)

                print(f"Processando arquivo: s3://{bucket_name}/{object_key}")
                s3_object = s3.get_object(Bucket=bucket_name, Key=object_key)
                file_content = s3_object['Body'].read().decode('utf-8')
                
                status_validacao_arquivo = "ERRO_VALIDACAO_ARQUIVO"
                detalhes_erro_arquivo = "ConteÃºdo do arquivo nÃ£o Ã© JSON vÃ¡lido."
                arquivo_data = None

                try:
                    arquivo_data = json.loads(file_content)
                    # ValidaÃ§Ã£o bÃ¡sica do schema do arquivo em si (nÃ£o dos pedidos ainda)
                    if isinstance(arquivo_data, dict) and 'lista_pedidos' in arquivo_data and isinstance(arquivo_data['lista_pedidos'], list):
                        status_validacao_arquivo = "ARQUIVO_VALIDADO"
                        detalhes_erro_arquivo = None
                        print(f"Schema do arquivo {object_key} validado.")

                        # Processar e enviar pedidos para a fila FIFO
                        for pedido_item in arquivo_data['lista_pedidos']:
                            # Simular transformaÃ§Ã£o/validaÃ§Ã£o do pedido_item
                            if 'id_pedido_arquivo' not in pedido_item or 'id_cliente_arquivo' not in pedido_item:
                                print(f"Pedido invÃ¡lido no arquivo {object_key}: {pedido_item}. Campos obrigatÃ³rios ausentes.")
                                # Poderia ter um tratamento de erro especÃ­fico para pedidos individuais dentro do arquivo
                                continue

                            pedido_formatado = {
                                'pedidoId': str(pedido_item.get('id_pedido_arquivo')),
                                'clienteId': str(pedido_item.get('id_cliente_arquivo')),
                                'itens': pedido_item.get('itens_pedido_arquivo', []),
                                'origem': 'S3_FILE',
                                'nomeArquivoOriginal': object_key,
                                'timestamp_extracao_arquivo': datetime.utcnow().isoformat() + "Z"
                            }
                            
                            try:
                                deduplication_id = f"{object_key}-{pedido_formatado['pedidoId']}-{uuid.uuid4()}" # Garante singularidade por tentativa
                                sqs.send_message(
                                    QueueUrl=SQS_FIFO_PEDIDOS_URL,
                                    MessageBody=json.dumps(pedido_formatado),
                                    MessageGroupId=str(pedido_formatado['pedidoId']), # Ou um group ID do arquivo
                                    MessageDeduplicationId=deduplication_id
                                )
                                print(f"Pedido {pedido_formatado['pedidoId']} do arquivo {object_key} enviado para SQS FIFO Pedidos.")
                            except Exception as sqs_e:
                                print(f"Erro ao enviar pedido do arquivo {object_key} para SQS FIFO: {str(sqs_e)}")
                                # Logar erro, talvez notificar, mas continuar processando outros pedidos/arquivos
                    else:
                        detalhes_erro_arquivo = "Schema do arquivo invÃ¡lido. Esperada chave 'lista_pedidos' contendo uma lista."
                        print(detalhes_erro_arquivo)
                
                except json.JSONDecodeError as je:
                    print(f"Arquivo {object_key} nÃ£o Ã© um JSON vÃ¡lido: {str(je)}")
                    # status_validacao_arquivo e detalhes_erro_arquivo jÃ¡ estÃ£o definidos

                # Registrar no DynamoDB o status do ARQUIVO
                item_to_put = {
                    'nomeArquivo': object_key,
                    'timestamp': datetime.utcnow().isoformat() + "Z",
                    'statusValidacao': status_validacao_arquivo, # Status do arquivo, nÃ£o dos pedidos individuais
                    'bucket': bucket_name
                }
                if detalhes_erro_arquivo:
                    item_to_put['detalhesErro'] = detalhes_erro_arquivo
                
                table.put_item(Item=item_to_put)

                # Notificar no SNS se houver erro no ARQUIVO
                if status_validacao_arquivo != "ARQUIVO_VALIDADO":
                    message_sns = f"Erro de validaÃ§Ã£o no ARQUIVO: {object_key} do bucket {bucket_name}.\n"
                    message_sns += f"Status: {status_validacao_arquivo}\n"
                    if detalhes_erro_arquivo:
                         message_sns += f"Detalhes: {detalhes_erro_arquivo}"
                    
                    sns.publish(
                        TopicArn=SNS_TOPIC_ARN,
                        Message=message_sns,
                        Subject=f"Erro de ValidaÃ§Ã£o de ARQUIVO S3 - {object_key}"
                    )
                    print(f"NotificaÃ§Ã£o de erro de ARQUIVO enviada para SNS para {object_key}")

        except Exception as e:
            err_msg = f"Erro crÃ­tico ao processar SQS record (body: {s3_event_message_body}): {str(e)}"
            if object_key: # Se jÃ¡ tivermos o nome do arquivo
                err_msg = f"Erro crÃ­tico ao processar SQS record para arquivo {object_key} (body: {s3_event_message_body}): {str(e)}"
            print(err_msg)
            try:
                sns.publish(TopicArn=SNS_TOPIC_ARN, Message=err_msg, Subject="Erro CrÃ­tico no Processamento de Arquivo S3")
            except Exception as sns_e:
                print(f"Falha ao enviar notificaÃ§Ã£o SNS sobre erro crÃ­tico: {str(sns_e)}")
            raise e
    return {'statusCode': 200, 'body': 'Processamento de arquivos S3 e envio de pedidos concluÃ­do'}
