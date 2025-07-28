import pandas as pd
import requests
import time
from tqdm import tqdm
import os

login = "e23b5220-da7e-414d-a773-242c0fce2c5d"
senha = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyIjp7ImlkIjo1LCJlbWFpbCI6ImJyeWFuLnNvdXphQGdydXBvcHJhbG9nLmNvbS5iciJ9LCJ0ZW5hbnQiOnsidXVpZCI6ImUyM2I1MjIwLWRhN2UtNDE0ZC1hNzczLTI0MmMwZmNlMmM1ZCJ9LCJpYXQiOjE3NTM3MTQzNDQsImV4cCI6MTc1Mzc1NzU0NH0.78l6ZXunNr2ONCB3rXcceExeu6NYBd-yzOAMWXWDAQc"  # continue seu token aqui

# Nome do arquivo final
ARQUIVO_XLSX = "saida_dados_regulares.xlsx"

def captura_de_dados_regulares( quinzena, current_page, total_de_paginas, carrie):
    # Remove o arquivo antigo, se existir
    if os.path.exists(ARQUIVO_XLSX):
        try:
            os.remove(ARQUIVO_XLSX)
            print(f"Arquivo antigo '{ARQUIVO_XLSX}' removido.")
        except Exception as e:
            print(f"Erro ao tentar apagar o arquivo existente: {e}")

    print("Iniciando captura de dados REGULARES...\n")

    # Aqui vamos guardar todos os dados
    tabela_geral = []

    for page in tqdm(range(current_page, total_de_paginas + 1), desc="Processando páginas", unit="página"):
        try:
            resposta = requests.get(
                "https://prafrota-be-bff-tenant-api.grupopra.tech/meli-pre-invoice-detail",
                params={
                    "page": page,
                    "limit": 1,
                    "isOwnCarrier": carrie,
                    "meliPeriodName": quinzena
                },
                headers={
                    "accept": "application/json",
                    "x-tenant-uuid": login,
                    "x-tenant-user-auth": senha
                },
                timeout=30
            )

            time.sleep(3)  # Dá uma pausa pra não sobrecarregar o servidor

            if resposta.status_code != 200:
                print(f"Erro na página {page}: Status {resposta.status_code}")
                continue

            json_resposta = resposta.json()

# 🚨 VERIFICAÇÃO de custo total zero
            if float(json_resposta.get('meliTotalCost', 0)) == 0:
                print(f"Página {page}: valor TOTAL de custo é ZERO. Ignorando processamento.")
                continue



            dados = json_resposta.get('data', [])
            if not dados:
                print(f"Página {page}: sem dados.")
                continue

            for linha in dados:
                tabela_geral.append({
                    'Total Recebido': linha.get('meliTotalCost'),
                    'Total Descontado': linha.get('meliTotalPenaltiesCost'),
                    'Operação': linha.get('meliStepType'),
                    'ID de Fatura': linha.get('meliPreInvoiceId'),
                    'Empresa': linha.get('meliProviderName'),
                    'Quinzena': linha.get('meliPeriodName'),
                    'Tipo de Fatura': linha.get('meliType')
                })

            # Converte a lista acumulada em DataFrame
            df_acumulado = pd.DataFrame(tabela_geral)

            # Salva tudo no Excel sobrescrevendo o conteúdo anterior
            with pd.ExcelWriter(ARQUIVO_XLSX, mode='w', engine='openpyxl') as writer:
                df_acumulado.to_excel(writer, sheet_name='Faturas', index=False)

        except Exception as e:
            print(f"Erro ao processar página {page}: {e}")
            continue

    print(f"\nTodos os dados foram salvos em '{ARQUIVO_XLSX}' com todas as páginas em uma única aba.")


def iniciar_fluxo():

    isonwcarrie = {"1": "true", "2": "false"}




    quinzena = input("Digite a Quinzena desejada (ex: 202505Q1): ")

    carrie = input("isonwcarrie?:\n1-true\n2-false: ")
    while carrie not in isonwcarrie:
        print("Seleção inválida!")
        carrie = input("isonwcarrie?:\n1-true\n2-false: ")
    carrie = isonwcarrie[carrie]

    print("\n===> A saída será em CSV incremental (stream)")
    print("Fluxo iniciado com sucesso.\n")
    return quinzena, carrie


def definir_paginacao(quinzena, login, senha, carrie):
    try:
        print("Definindo paginação...")
        requisicao_controller = requests.get(
            "https://prafrota-be-bff-tenant-api.grupopra.tech/meli-pre-invoice-detail",
            params={
                "page": 1,
                "limit": 1,
                "isOwnCarrier": carrie,
                "meliPeriodName": quinzena
            },
            headers={
                "accept": "application/json",
                "x-tenant-uuid": login,
                "x-tenant-user-auth": senha
            },
            timeout=30
        )
        data = requisicao_controller.json()
        total_de_paginas = data.get('pageCount')
        current_page = data.get('currentPage')
        print(f"Total de páginas: {total_de_paginas}\n")
        return total_de_paginas, current_page
    except Exception as e:
        print(f"Erro ao definir paginação: {e}")
        return 0, 0
