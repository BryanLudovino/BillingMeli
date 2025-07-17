import pandas as pd
import requests
import openpyxl
import time
import re

login = "e23b5220-da7e-414d-a773-242c0fce2c5d"
senha = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyIjp7ImlkIjo1LCJlbWFpbCI6ImJyeWFuLnNvdXphQGdydXBvcHJhbG9nLmNvbS5iciJ9LCJ0ZW5hbnQiOnsidXVpZCI6ImUyM2I1MjIwLWRhN2UtNDE0ZC1hNzczLTI0MmMwZmNlMmM1ZCJ9LCJpYXQiOjE3NTI3NjYzMzMsImV4cCI6MTc1MjgwOTUzM30.Ew93QOe8HhJB0RLFRxZYZ5z6rvq4iXh2s2XNOaYBikg"

def iniciar_fluxo():
    tipo_de_regulares = {"1": "Regular", "2": "Complementaria"}
    isonwcarrie = {"1":"true","2":"false"}
    tipo = input("Bem vindo à Captura processada de Faturas do Meli\nPor favor, digite o tipo de fatura que você deseja:\n1-Regular\n2-Complementar  ")
    while tipo not in tipo_de_regulares:
        print("Tipo Inválido!!")
        tipo = input("Por favor, digite o tipo de fatura que você deseja (Regular ou Complementar):  ")
    tipo = tipo_de_regulares[tipo]
    quinzena = input("Agora digite a Quinzena desejada (ex: 202505Q1): ")
    carrie = input("isonwcarrie?: \n1-true\n2-false")
    while carrie not in isonwcarrie:
        print("Seleção Inválida")
        carrie = input("isonwcarrie?: \n1-true\n2-false")
    carrie = isonwcarrie[carrie]
    print("Etapa finalizada de Fluxo Finalizada com sucesso")
    return quinzena, tipo, carrie

def definir_paginacao(tipo, quinzena,login,senha,carrie):
    try:
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
            }
        )
        data = requisicao_controller.json()
        total_de_paginas = data.get('pageCount')
        current_page = data.get('currentPage')
        return total_de_paginas, current_page
        print("Paginação definida")
    except Exception as e:
        print(f"Erro ao definir paginação: {e}")
        return total_de_paginas,current_page

def captura_de_dados_regulares(tipo, quinzena, current_page, total_de_paginas,carrie):
    tabela = []
    print(f"Iniciando captura de dados regulares: {total_de_paginas} páginas totais")
    
    page = current_page  # Variável para controlar o loop
    
    while page <= total_de_paginas:
        print(f"Processando página {page} de {total_de_paginas}")
        
        try:
            resposta = requests.get(
                "https://prafrota-be-bff-tenant-api.grupopra.tech/meli-pre-invoice-detail",
                params={
                    "page": page,  # Usa a variável do loop, minúsculo
                    "limit": 1,
                    "meliType": tipo,
                    "isOwnCarrier": carrie,
                    "meliPeriodName": quinzena
                },
                headers={
                    "accept": "application/json",
                    "x-tenant-uuid": login,
                    "x-tenant-user-auth": senha
                }
            )
            
            if resposta.status_code != 200:
                print(f"Erro na página {page}: Status {resposta.status_code}")
                page += 1
                continue
                
            json_resposta = resposta.json()
            dados = json_resposta.get('data', [])
            
            if not dados:
                print(f"Página {page}: Sem dados")
                page += 1
                continue
            
            registros_pagina = 0
            for linha in dados:
                meli_data = linha.get('meliData', {})
                items = meli_data.get('items', [])
                
                if not items:
                    continue
                    
                for details in items:
                    detail = details.get("details", [])
                    
                    if not detail:
                        # Se não há details, cria um registro com dados básicos
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
                            "cost": float(details.get("total_cost", 0) or 0)
                        })
                        registros_pagina += 1
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
                            "cost": float(det.get("cost", 0) or 0)
                        })
                        registros_pagina += 1
            
            print(f"Página {page}: {registros_pagina} registros processados")
            time.sleep(1)  # Pausa entre requisições
            page += 1  # Incrementa a variável do loop
            
        except Exception as e:
            print(f"Erro ao processar página {page}: {e}")
            page += 1
            continue
    
    print(f"Captura finalizada: {len(tabela)} registros totais")
    return tabela


def filtrar_tabelas(tabela):
    df = pd.DataFrame(tabela)
    if 'type_len' not in df.columns:
        print("Coluna 'type_len' não encontrada! Colunas disponíveis:", df.columns)
        return df, pd.DataFrame(), pd.DataFrame()
    
    # Criar cópias explícitas
    rotas = df[df['type_len'] == "service"].copy()
    penalidades = df[df['type_len'] == "penalty"].copy()
    adicionais = df[df['type_len'] == "additional"].copy()
    
    return rotas, penalidades, adicionais

def tabela_rotas(rotas):
    lista_km = [
        "1/100", "101/150", "0/100", "151/200", "201/250", "251/300", "51/100", "0/50",
        "201/300", "301/99999", "1/50", "0/0", "0/25", "26/50", "51/75", "76/100",
        "301/400", "401/500", "501/600", "601/700", "701/800", "801/900", "901/1000",
        "1001/1100", "1101/1200", "1201/1300", "1301/1400", "1401/1500"
    ]
    regex_km = "|".join([re.escape(km) for km in lista_km])
    if 'Description_route' in rotas.columns:
        rotas['Description_route'] = rotas['Description_route'].astype(str)
        rotas['km'] = rotas['Description_route'].str.extract(f"({regex_km})", flags=re.IGNORECASE)
        rotas['km'] = rotas['km'].fillna("0/0")
        rotas['km'] = rotas['km'].str.replace('0/100','1/100')
        rotas['ambulance'] = rotas['Description_route'].str.contains("AMBULANCE", case=False, na=False).map({True: 'Yes', False: 'No'})
        rotas['part_of_time'] = rotas['Description_route'].str.contains("TIME", case=False, na=False).map({True: 'Yes', False: 'No'})
        rotas['holiday'] = rotas['Description_route'].str.contains("HOLYDAY", case=False, na=False).map({True: 'Yes', False: 'No'})
        rotas[['type_route', 'part2']] = rotas['Description_route'].str.split(' - SVC:', n=1, expand=True)
        rotas['service'] = rotas['part2'].str.split(' - ',expand=True)[0].str.strip()
        rotas.drop(columns= ['Description_route','type_len','part2'],inplace= True)
        rotas = rotas[['company','id_invoiceId','detail_route_id','init_date','finish_date','operation','two_weeks','type_route','service','license_plate','driver_name','km','ambulance','part_of_time','holiday','tax_iss','tax_icms','cost']]

    else:
        rotas['km'] = "Sem valor"
        rotas['ambulance'] = 'No'
        rotas['part_of_time'] = 'No'
        rotas['holiday'] = 'No'
    # Remover manipulação da coluna 'lixo' se ela não existir
    if 'lixo' in rotas.columns:
        rotas[['Parte Lixo 1', 'Parte Lixo 2']] = rotas['lixo'].str.split(' - ', n=1, expand=True)
        rotas.drop(columns=['lixo'], inplace=True)
    return rotas

def tabela_descontos(penalidades):
    if 'Description_route' in penalidades.columns:
        penalidades['Description_route'] = penalidades['Description_route'].str.replace('>', ':', regex=False)
        penalidades[['reason', 'dados']] = penalidades['Description_route'].str.split(': ', n=1, expand=True)
        penalidades[['placa', 'datax', 'id Packages']] = penalidades['dados'].str.split(' ', n=2, expand=True)
        drop_cols = [col for col in ['placa', 'datax', 'Description_route', 'tax_iss', 'tax_icms', 'dados', 'type_len', 'amount'] if col in penalidades.columns]
        penalidades.drop(columns=drop_cols, inplace=True)
        return penalidades

def tabela_adicionais(adicionais):
    # Se a tabela estiver vazia, retorna sem processar
    if adicionais.empty:
        return adicionais
    
    if 'Description_route' in adicionais.columns:
        # Faz o split e garante que sempre há 2 colunas
        split_result = adicionais['Description_route'].str.split(' - ', n=1, expand=True)
        
        # Se só há 1 coluna (não encontrou ' - '), cria a segunda coluna vazia
        if len(split_result.columns) == 1:
            split_result[1] = None
        
        # Atribui as colunas
        adicionais['reason'] = split_result[0]
        adicionais['excluir'] = split_result[1]
        
        # Remove colunas desnecessárias
        drop_cols = [col for col in ['tax_iss', 'tax_icms', 'type_len', 'excluir', 'Description_route'] if col in adicionais.columns]
        adicionais.drop(columns=drop_cols, inplace=True)
    
    return adicionais

def regular_para_excel(rotas, penalidades, adicionais, quinzena, carrie):
    if carrie == "true":
        with pd.ExcelWriter(f"C:/Users/Bryan Souza/Nextcloud/Contas a Receber - Pralog/Mercado Livre/Billing_controll/Regulares/Fatura Regular Quinzena {quinzena}.xlsx", engine="openpyxl") as writer:
            rotas.to_excel(writer, sheet_name="Rotas", index=False)
            penalidades.to_excel(writer, sheet_name="Descontos", index=False)
        adicionais.to_excel(f"C:/Users/Bryan Souza/Nextcloud/Contas a Receber - Pralog/Mercado Livre/Billing_controll/Adicionais/Adicional Regular {quinzena}.xlsx" ,index=False)
        print(f"Planilha Fatura Regular Quinzena {quinzena}, salva com sucesso!!")
    else:
        with pd.ExcelWriter(f"C:/Users/Bryan Souza/Documents/Devizinho_do_mal/Fatura Regular Quinzena {quinzena}.xlsx", engine="openpyxl") as writer:
            rotas.to_excel(writer, sheet_name="Rotas", index=False)
            penalidades.to_excel(writer, sheet_name="Descontos", index=False)
        print(f"Planilha Fatura Regular Quinzena {quinzena}, salva com sucesso!")



def captura_de_dados_complementares(tipo, quinzena, current_page, total_de_paginas,carrie):
    tabela = []
    print(f"Iniciando captura de dados complementares: {total_de_paginas} páginas totais")
    
    page = current_page
    while page <= total_de_paginas:
        print(f"Processando página {page} de {total_de_paginas}")
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
                }
            )
            if resposta.status_code != 200:
                print(f"Erro na página {page}: Status {resposta.status_code}")
                page += 1
                continue

            json_resposta = resposta.json()
            dados = json_resposta.get('data', [])

            if not dados:
                print(f"Página {page}: Sem dados")
                page += 1
                continue

            registros_pagina = 0
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
                        "cost": None,
                        "pay_or_received":None
                    })
                    registros_pagina += 1
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
                        registros_pagina += 1

            print(f"Página {page}: {registros_pagina} registros processados")
            time.sleep(1)
            page += 1

        except Exception as e:
            print(f"Erro ao processar página {page}: {e}")
            page += 1
            continue
    print(f"Captura finalizada: {len(tabela)} registros totais")
    return tabela

def complementar_para_excel(tabela, quinzena,carrier):
    if carrier =="true":

        complementares = pd.DataFrame(tabela)
        complementares.to_excel(f"C:/Users/Bryan Souza/Nextcloud/Contas a Receber - Pralog/Mercado Livre/Análises Meli/Billing_controll/Complementares/Fatura_Complementar_Quinzena_{quinzena}.xlsx", index=False)
        
    else:
        complementares = pd.DataFrame(tabela)
        complementares.to_excel(f"C:/Users/Bryan Souza/Documents/Devizinho_do_malcomply/Fatura_Complementar_Quinzena_{quinzena}.xlsx", index=False)