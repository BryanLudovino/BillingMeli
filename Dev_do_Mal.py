import AnáliseDiretoria
from AnáliseDiretoria import login, senha

# Inicia o fluxo e obtém as configurações
quinzena, carrie = AnáliseDiretoria.iniciar_fluxo()

# Define paginação (quantas páginas no total)
total_paginas, pagina_atual = AnáliseDiretoria.definir_paginacao( quinzena, login, senha, carrie)

AnáliseDiretoria.captura_de_dados_regulares(quinzena,pagina_atual,total_paginas,carrie,)