import os
import pyodbc

def criar_tabela(cursor, nome_tabela, colunas_dict):
    colunas = [f'[{col}] {tipo}' for col, tipo in colunas_dict.items()]
    sql = f"CREATE TABLE {nome_tabela} ({', '.join(colunas)})"
    try:
        cursor.execute(sql)
        print(f'Tabela {nome_tabela} criada.')
    except Exception:
        print(f'Tabela {nome_tabela} já existe.')

def teste_criacao_tabelas_access():
    desktop = os.path.join(os.path.expanduser("~"), "Desktop")
    caminho_banco = os.path.join(desktop, "Basedev.accdb")
    conn_str = (
        r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
        f'DBQ={caminho_banco};'
    )
    # Definição fiel ao Functions.py
    tabelas_dict = {
        "Rotas": {
            "company": "TEXT(255)", "id_invoiceId": "DOUBLE", "detail_route_id": "TEXT(255)", "init_date": "TEXT(50)", "finish_date": "TEXT(50)", "operation": "TEXT(50)", "two_weeks": "TEXT(50)", "type_route": "TEXT(255)", "service": "TEXT(255)", "license_plate": "TEXT(50)", "driver_name": "TEXT(255)", "km": "TEXT(50)", "ambulance": "TEXT(10)", "part_of_time": "TEXT(10)", "holiday": "TEXT(10)", "tax_iss": "DOUBLE", "tax_icms": "DOUBLE", "cost": "DOUBLE"
        },
        "Penalidades": {
            "company": "TEXT(255)",
            "id_invoiceId": "DOUBLE",
            "detail_route_id": "TEXT(255)",
            "init_date": "TEXT(50)",
            "finish_date": "TEXT(50)",
            "operation": "TEXT(50)",
            "two_weeks": "TEXT(50)",
            "license_plate": "TEXT(50)",
            "driver_name": "TEXT(255)",
            "cost": "DOUBLE",
            "reason": "TEXT(255)",
            "id Packages": "TEXT(255)"
        },
        "Adicionais": {
            "company": "TEXT(255)",
            "id_invoiceId": "DOUBLE",
            "detail_route_id": "TEXT(255)",
            "init_date": "TEXT(50)",
            "finish_date": "TEXT(50)",
            "operation": "TEXT(50)",
            "two_weeks": "TEXT(50)",
            "license_plate": "TEXT(50)",
            "driver_name": "TEXT(255)",
            "amount": "DOUBLE",
            "cost": "DOUBLE",
            "reason": "TEXT(255)"
        },
        "Complementares": {
            "id_billing": "DOUBLE", "company": "TEXT(255)", "operation": "TEXT(50)", "two_weeks": "TEXT(50)", "description": "TEXT(255)", "payment": "TEXT(255)", "pay_or_received": "TEXT(255)", "cost": "DOUBLE"
        }
    }
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        for nome_tabela, colunas_dict in tabelas_dict.items():
            criar_tabela(cursor, nome_tabela, colunas_dict)
        conn.commit()
        cursor.close()
        conn.close()
        print('Todas as tabelas estão criadas com as colunas fiéis ao Functions.py.')
    except Exception as e:
        print(f'Erro ao criar tabelas: {e}')

if __name__ == "__main__":
    teste_criacao_tabelas_access() 