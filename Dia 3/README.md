Nas aulas anteriores, estabelecemos dois canais de entrada para pedidos (API e S3) que, após validação, resultam na publicação de um evento NovoPedidoValidado no nosso Custom event bus no Amazon EventBridge.
Nesta terceira aula, vamos construir a lógica que consome esses eventos. Configuraremos uma regra no EventBridge para capturar os eventos de NovoPedidoValidado. Essa regra direcionará os eventos para uma nova fila SQS Standard, que servirá como um buffer para o processamento principal. Uma nova função Lambda será acionada por esta fila, simulará a lógica de "processamento do pedido" (ex: verificação de inventário, cálculo de frete, etc. - que simplificaremos) e, finalmente, persistirá os detalhes e o status do pedido processado em uma nova tabela principal do Amazon DynamoDB.

Ao final desta aula, você terá:
Uma regra no EventBridge para rotear eventos de novos pedidos validados.
Uma fila SQS Standard para desacoplar o processamento principal dos pedidos.
Uma função Lambda para executar a lógica de processamento central do pedido.
Uma tabela DynamoDB principal para armazenar o estado e os detalhes dos pedidos processados.
A conexão efetiva entre a validação do pedido e sua persistência final.
