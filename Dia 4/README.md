Bem-vindos à nossa última aula! Nas sessões anteriores, construímos os fluxos de ingestão de pedidos via API e S3, a validação desses pedidos, a publicação de eventos no EventBridge, e o processamento central com persistência no DynamoDB.
Nesta aula final, vamos expandir a funcionalidade do nosso sistema para lidar com outras operações importantes no ciclo de vida de um pedido: cancelamento e alteração. Utilizaremos novamente o EventBridge para rotear esses novos tipos de eventos para filas SQS dedicadas, que acionarão Lambdas específicas para atualizar o estado do pedido na nossa tabela DynamoDB principal. Além disso, revisaremos a importância das Dead Letter Queues (DLQs) que configuramos ao longo do caminho e faremos um teste prático para ver uma DLQ em ação.

Ao final desta aula, você terá:
Fluxos funcionais para cancelamento e alteração de pedidos, integrados à arquitetura existente.
Novas regras no EventBridge, filas SQS e funções Lambda para essas operações.
Uma compreensão prática do funcionamento e da importância das DLQs.
Uma visão completa da arquitetura integrada e funcional.
