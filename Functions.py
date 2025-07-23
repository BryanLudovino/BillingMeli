import pandas as pd
import requests
import time
import re
from tqdm import tqdm
import numpy as np


login = "e23b5220-da7e-414d-a773-242c0fce2c5d"
senha = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyIjp7ImlkIjo1LCJlbWFpbCI6ImJyeWFuLnNvdXphQGdydXBvcHJhbG9nLmNvbS5iciJ9LCJ0ZW5hbnQiOnsidXVpZCI6ImUyM2I1MjIwLWRhN2UtNDE0ZC1hNzczLTI0MmMwZmNlMmM1ZCJ9LCJpYXQiOjE3NTMyODkzMDYsImV4cCI6MTc1MzMzMjUwNn0.LmYlpddqRTKXGoH2c0UM1x-8O-_9dDkbp7Xt-jp0xlo"


def iniciar_fluxo():
    tipo_de_regulares = {"1": "Regular", "2": "Complementaria"}
    isonwcarrie = {"1": "true", "2": "false"}

    tipo = input("Bem vindo à Captura processada de Faturas do Meli\n"
                 "Digite o tipo de fatura:\n1-Regular\n2-Complementar: ")
    while tipo not in tipo_de_regulares:
        print("Tipo inválido!")
        tipo = input("Digite 1 para Regular ou 2 para Complementar: ")
    tipo = tipo_de_regulares[tipo]

    quinzena = input("Digite a Quinzena desejada (ex: 202505Q1): ")

    carrie = input("isonwcarrie?:\n1-true\n2-false: ")
    while carrie not in isonwcarrie:
        print("Seleção inválida!")
        carrie = input("isonwcarrie?:\n1-true\n2-false: ")
    carrie = isonwcarrie[carrie]

    print("\n===> A saída será em CSV incremental (stream)")
    print("Fluxo iniciado com sucesso.\n")
    return quinzena, tipo, carrie


def definir_paginacao(tipo, quinzena, login, senha, carrie):
    try:
        print("Definindo paginação...")
        requisicao_controller = requests.get(
            "https://prafrota-be-bff-tenant-api.grupopra.tech/meli-pre-invoice-detail",
            params={
                "page": 1,
                "limit": 1,
                "meliType": tipo,
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


def captura_de_dados_regulares(tipo, quinzena, current_page, total_de_paginas, carrie):
    lista_km = [
        "1/100", "101/150", "0/100", "151/200", "201/250", "251/300", "51/100", "0/50",
        "201/300", "301/99999", "1/50", "0/0", "0/25", "26/50", "51/75", "76/100",
        "301/400", "401/500", "501/600", "601/700", "701/800", "801/900", "901/1000",
        "1001/1100", "1101/1200", "1201/1300", "1301/1400", "1401/1500"
    ]

    arquivos = {
        "rotas": f"C:/Users/Bryan Souza/Desktop/Projeto Dev do mal/Regulares/{quinzena}.csv",
        "penalidades": f"C:/Users/Bryan Souza/Desktop/Projeto Dev do mal/Penalidades/{quinzena}.csv",
        "adicionais": f"C:/Users/Bryan Souza/Desktop/Projeto Dev do mal/Adiconais/{quinzena}.csv"
    }

    # Remove arquivos antigos se existirem
    for arq in arquivos.values():
        try:
            open(arq, 'w').close()
        except:
            pass

    print("Iniciando captura de dados REGULARES...\n")
    for page in tqdm(range(current_page, total_de_paginas + 1), desc="Processando páginas", unit="página"):
        try:
            resposta = requests.get(
                "https://prafrota-be-bff-tenant-api.grupopra.tech/meli-pre-invoice-detail",
                params={
                    "page": page,
                    "limit": 1,
                    "meliType": tipo,
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
            time.sleep(3)

            if resposta.status_code != 200:
                print(f"Erro na página {page}: Status {resposta.status_code}")
                continue

            json_resposta = resposta.json()
            dados = json_resposta.get('data', [])
            if not dados:
                print(f"Página {page}: sem dados.")
                continue

            tabela = []
            for linha in dados:
                meli_data = linha.get('meliData', {})
                items = meli_data.get('items', [])

                if not items:
                    tabela.append({
                        "company": linha.get("meliProviderName"),
                        "id_invoiceId": int(linha.get("meliPreInvoiceId", 0)) if linha.get("meliPreInvoiceId") else None,
                        "detail_route_id": None,
                        "init_date": None,
                        "finish_date": None,
                        "operation": linha.get("meliStepType"),
                        "two_weeks": linha.get("meliPeriodName"),
                        "Description_route": None,
                        "license_plate": None,
                        "driver_name": None,
                        "type_len": None,
                        "tax_iss": 0,
                        "tax_icms": 0,
                        "amount": 0,
                        "cost": 0,
                        "custo": 0
                    })
                    continue

                for details in items:
                    detail = details.get("details", [])
                    if not detail:
                        tabela.append({
                            "company": linha.get("meliProviderName"),
                            "id_invoiceId": int(linha.get("meliPreInvoiceId", 0)) if linha.get("meliPreInvoiceId") else None,
                            "detail_route_id": None,
                            "init_date": None,
                            "finish_date": None,
                            "operation": linha.get("meliStepType"),
                            "two_weeks": linha.get("meliPeriodName"),
                            "Description_route": details.get("description"),
                            "license_plate": None,
                            "driver_name": None,
                            "type_len": details.get("item_type", {}).get("name"),
                            "tax_iss": 0,
                            "tax_icms": 0,
                            "amount": 0,
                            "cost": float(details.get("total_cost", 0) or 0),
                            "custo": float(details.get("cost", 0) or 0)
                        })
                        continue

                    for det in detail:
                        tabela.append({
                            "company": linha.get("meliProviderName"),
                            "id_invoiceId": int(linha.get("meliPreInvoiceId", 0)) if linha.get("meliPreInvoiceId") else None,
                            "detail_route_id": det.get("external_route_id"),
                            "init_date": det.get("init_date"),
                            "finish_date": det.get("finish_date"),
                            "operation": linha.get("meliStepType"),
                            "two_weeks": linha.get("meliPeriodName"),
                            "Description_route": details.get("description"),
                            "license_plate": det.get("vehicle_license_plate"),
                            "driver_name": det.get("driver_name"),
                            "type_len": details.get("item_type", {}).get("name"),
                            "tax_iss": float(det.get("tax_iss", 0) or 0),
                            "tax_icms": float(det.get("tax_icms", 0) or 0),
                            "amount": int(det.get("amount", 0) or 0),
                            "cost": float(det.get("cost", 0) or 0),
                            "custo": float(details.get("cost", 0) or 0)
                        })

            # Tratamento dos DataFrames
            df = pd.DataFrame(tabela)
            rotas = df[df['type_len'] == "service"].copy()
            penalidades = df[df['type_len'] == "penalty"].copy()
            adicionais = df[df['type_len'].isin(["additional", "refund"])].copy()

            # TRATAMENTO ROTAS
            if not rotas.empty:
                regex_km = "|".join([re.escape(km) for km in lista_km])
                rotas['Description_route'] = rotas['Description_route'].astype(str)
                rotas['km'] = rotas['Description_route'].str.extract(f"({regex_km})", flags=re.IGNORECASE)
                rotas['km'] = rotas['km'].fillna("0/0").str.replace('0/100', '1/100')
                rotas['ambulance'] = rotas['Description_route'].str.contains("AMBULANCE", case=False, na=False).map({True: 'Yes', False: 'No'})
                rotas['part_of_time'] = rotas['Description_route'].str.contains("TIME", case=False, na=False).map({True: 'Yes', False: 'No'})
                rotas['holiday'] = rotas['Description_route'].str.contains("HOLYDAY", case=False, na=False).map({True: 'Yes', False: 'No'})
                rotas[['type_route', 'part2']] = rotas['Description_route'].str.split(' - SVC:', n=1, expand=True)
                rotas['service'] = rotas['part2'].str.split(' - ', expand=True)[0].str.strip()
                rotas.drop(columns=['Description_route', 'type_len', 'part2', 'custo'], inplace=True)

            # TRATAMENTO PENALIDADES
            if not penalidades.empty:
                penalidades['Description_route'] = penalidades['Description_route'].str.replace('> ', ':', regex=False)
                penalidades[['reason', 'dados']] = penalidades['Description_route'].str.split(': ', n=1, expand=True)
                penalidades[['placa', 'datax', 'A']] = penalidades['dados'].str.split(' ', n=2, expand=True)
                penalidades['id Packages'] = penalidades.apply(
                    lambda row: row['datax'] if row['reason'] == 'Pnr Packages Penalty'
                    else row['A'] if row['reason'] == 'Lost Packages Penalty'
                    else np.nan, axis=1
                )
                penalidades.drop(columns=[col for col in ['placa', 'datax', 'Description_route', 'dados', 'type_len', 'amount', 'A'] if col in penalidades.columns], inplace=True)

            # TRATAMENTO ADICIONAIS
            if not adicionais.empty:
                split_result = adicionais['Description_route'].str.split('-', n=1, expand=True)
                if len(split_result.columns) == 1:
                    split_result[1] = None
                adicionais['reason'] = split_result[0].str.strip()
                adicionais.drop(columns=[col for col in ['tax_iss', 'tax_icms', 'type_len', 'Description_route', 'cost'] if col in adicionais.columns], inplace=True)

            # SALVAR CSV incremental
            rotas.to_csv(arquivos["rotas"], mode='a', header=(page == 1), index=False)
            penalidades.to_csv(arquivos["penalidades"], mode='a', header=(page == 1), index=False)
            adicionais.to_csv(arquivos["adicionais"], mode='a', header=(page == 1), index=False)

        except Exception as e:
            print(f"Erro na página {page}: {e}")


def captura_de_dados_complementares(tipo, quinzena, current_page, total_de_paginas, carrie):
    arquivo_complementar = f"C:/Users/Bryan Souza/Desktop/Projeto Dev do mal/Complementares/{quinzena}.csv"
    open(arquivo_complementar, 'w').close()  # Limpa arquivo antigo

    print("Iniciando captura de dados COMPLEMENTARES...\n")
    for page in tqdm(range(current_page, total_de_paginas + 1), desc="Processando páginas", unit="página"):
        try:
            resposta = requests.get(
                "https://prafrota-be-bff-tenant-api.grupopra.tech/meli-pre-invoice-detail",
                params={
                    "page": page,
                    "limit": 1,
                    "meliType": tipo,
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
            time.sleep(3)

            if resposta.status_code != 200:
                print(f"Erro na página {page}: Status {resposta.status_code}")
                continue

            json_resposta = resposta.json()
            dados = json_resposta.get('data', [])
            if not dados:
                print(f"Página {page}: sem dados.")
                continue

            tabela = []
            for linha in dados:
                meli_data = linha.get('meliData', {})
                items = meli_data.get('items', [])
                if not items:
                    tabela.append({
                        "id_billing": float(linha.get('meliPreInvoiceId', 0)),
                        "company": linha.get('meliProviderName'),
                        "operation": linha.get('meliStepType'),
                        "two_weeks": linha.get('meliPeriodName'),
                        "description": None,
                        "payment": None,
                        "pay_or_received": None,
                        "cost": None
                    })
                else:
                    for item in items:
                        tabela.append({
                            "id_billing": int(linha.get('meliPreInvoiceId', 0)),
                            "company": linha.get('meliProviderName'),
                            "operation": linha.get('meliStepType'),
                            "two_weeks": linha.get('meliPeriodName'),
                            "description": item.get('description'),
                            "payment": (item.get('item_type') or {}).get('name'),
                            "pay_or_received": (item.get('item_type') or {}).get('operation'),
                            "cost": float(item.get('total_cost', 0))
                        })

            complementares = pd.DataFrame(tabela)
            complementares.to_csv(arquivo_complementar, mode='a', header=(page == 1), index=False)

        except Exception as e:
            print(f"Erro na página {page}: {e}")
