# Guia de Uso - Meli_Swagger

Este projeto automatiza a extração e organização de dados de faturas do Mercado Livre (Meli) utilizando a API do Grupo Pralog. O resultado é exportado em planilhas Excel, separando faturas regulares e complementares.

## Pré-requisitos

- Python 3.8 ou superior
- Instalar as bibliotecas necessárias:
  - `requests`
  - `pandas`
  - `openpyxl`

Você pode instalar as dependências com o comando:

```
pip install requests pandas openpyxl
```

## Como utilizar

1. Abra o terminal na pasta `Meli_Swagger`.
2. Execute o arquivo principal:

```
python Programa.py
```

3. Siga as instruções no terminal:
   - Escolha o tipo de fatura: `1` para Regular ou `2` para Complementar.
   - Informe a quinzena desejada (exemplo: `202505Q1`).

O script irá processar os dados e salvar os resultados em arquivos Excel nas seguintes pastas:

- Faturas **Regulares**:  
  `C:/Users/Bryan Souza/Nextcloud/Contas a Receber - Pralog/Mercado Livre/Análises Meli/Billing_controll/Regulares/`
- Faturas **Complementares**:  
  `C:/Users/Bryan Souza/Nextcloud/Contas a Receber - Pralog/Mercado Livre/Análises Meli/Billing_controll/Complementares/`

## Observações
- Os arquivos de saída são nomeados conforme a quinzena informada.
- Os dados de autenticação (login e senha) já estão definidos no código, mas podem ser alterados no arquivo `Functions.py` se necessário.
- O script faz requisições à API do Grupo Pralog, portanto é necessário ter acesso à internet.

## Estrutura dos arquivos
- `Programa.py`: Script principal, responsável pelo fluxo de execução.
- `Functions.py`: Funções auxiliares para requisições, tratamento de dados e exportação para Excel.

---
Dúvidas ou sugestões? Entre em contato com o desenvolvedor responsável. 