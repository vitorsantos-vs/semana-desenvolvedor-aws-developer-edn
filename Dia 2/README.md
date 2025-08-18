Na aula anterior, estabelecemos o fluxo de ingestão de pedidos via API. Nesta segunda aula, vamos implementar um canal alternativo para a entrada de pedidos: o processamento de arquivos JSON enviados para um bucket S3. Esta funcionalidade é crucial para cenários onde pedidos são recebidos em lote ou de sistemas externos que geram arquivos.
O objetivo é configurar um bucket S3. Quando um novo arquivo JSON for carregado, uma notificação do S3 enviará um evento para uma fila SQS Standard. Uma função Lambda será acionada por esta fila, fará o download do arquivo, validará seu conteúdo e, crucialmente, se o arquivo contiver dados de pedidos válidos, esses pedidos serão transformados e enviados para a mesma Amazon SQS FIFO Pedidos que é utilizada pelo fluxo da API (criada na Aula 1). Além disso, o resultado da validação do arquivo será registrado em uma tabela DynamoDB e, em caso de erro na validação do arquivo, uma notificação será enviada via SNS.

Ao final desta aula, você terá:
Um bucket S3 configurado para receber arquivos JSON de pedidos.
Uma fila SQS Standard para desacoplar o processamento inicial dos arquivos.
Uma função Lambda para validar arquivos JSON do S3, extrair pedidos e enviá-los para o pipeline principal.
Uma tabela DynamoDB para rastrear o histórico de validação de arquivos.
Um tópico SNS para notificações de erro na validação de arquivos.
A integração efetiva de uma segunda fonte de pedidos no sistema.
