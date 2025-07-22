import requests
import json
import Functions
import pandas as pd
from Functions import login
from Functions import senha

quinzena, tipo, carrie, saida = Functions.iniciar_fluxo()

if tipo == "Regular":
    paginas_totais, pagina_atual = Functions.definir_paginacao(tipo, quinzena, login, senha, carrie)
    matriz = Functions.captura_de_dados_regulares(tipo, quinzena, pagina_atual, paginas_totais, carrie)
    rotas, penalidades, adicionais = Functions.filtrar_tabelas(matriz)
    routas_billing = Functions.tabela_rotas(rotas)
    descontos = Functions.tabela_descontos(penalidades)
    adicional = Functions.tabela_adicionais(adicionais)
    if saida == "1":
        Functions.regular_para_excel(routas_billing, descontos, adicionais, quinzena, carrie)
    else:
        # Salvar em CSV ao invés de Access
        Functions.regular_para_CSV(routas_billing, descontos, adicional, quinzena, carrie)
else:
    paginas_totais, pagina_atual = Functions.definir_paginacao(tipo, quinzena, login, senha, carrie)
    matriz = Functions.captura_de_dados_complementares(tipo, quinzena, pagina_atual, paginas_totais, carrie)
    if saida == "1":
        Functions.complementar_para_excel(matriz, quinzena, carrie)
    else:
        # Salvar em CSV ao invés de Access
        Functions.complementar_para_csv(matriz, quinzena, carrie)
