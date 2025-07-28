import Functions
from Functions import login, senha

# Inicia o fluxo e obtém as configurações
quinzena, tipo, carrie = Functions.iniciar_fluxo()

# Define paginação (quantas páginas no total)
total_paginas, pagina_atual = Functions.definir_paginacao(tipo, quinzena, login, senha, carrie)

# Se não encontrar páginas válidas, encerra o programa
if total_paginas == 0:
    print("Não foi possível definir a paginação. Verifique os parâmetros e tente novamente.")
    exit()

# Escolhe o fluxo de acordo com o tipo
if tipo == "Regular":
    print("Executando fluxo de Regulares...")
    Functions.captura_de_dados_regulares(tipo, quinzena, pagina_atual, total_paginas, carrie)
else:
    print("Executando fluxo de Complementares...")
    Functions.captura_de_dados_complementares(tipo, quinzena, pagina_atual, total_paginas, carrie)

print("\n✅ Captura concluída! Os arquivos CSV foram salvos com sucesso.")
