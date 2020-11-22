import pandas as pd
import numpy as np
from funcoes_microdados import ler_microdados, ler_escolas_censo

pd.set_option('display.max_columns', 500)
#pd.set_option('display.max_rows', 500)

# Leitura dos dados: MICRODADOS_ENEM são os microdados do ENEM, ESCOLAS.csv é uma tabela do Censo Escolar do respectivo ano
ano = 2019

escolas = ler_escolas_censo(ano, f'./dados/{ano}/ESCOLAS_{ano}.csv')
dados = ler_microdados(ano, f'./dados/{ano}/MICRODADOS_ENEM_{ano}.csv')

# Função que retorna ranking dataframe com filtro de número mínimo de alunos da escola (padrão 10 alunos mínimos)
def medias_escolas(df, min_alunos = 10):
    geral = df[df.PARTICIPANTES >= min_alunos] # Seleciona somente os alunos cuja escola tem número de participantes >= min_alunos
    # Calcula as médias por escola mantendo outras variáveis de escola
    geral = geral.groupby('CO_ESCOLA').agg(CH = ('NU_NOTA_CH', 'mean'),CN = ('NU_NOTA_CN', 'mean'),  LC = ('NU_NOTA_LC', 'mean'), MT = ('NU_NOTA_MT', 'mean'),
                                           MEDIAS_OBJETIVA = ('OBJETIVA', 'mean'), RED = ('NU_NOTA_REDACAO', 'mean'), OBJETIVAS_COM_RED = ('MEDIA_RD', 'mean'),
                                           MUNICIPIO = ('NO_MUNICIPIO_ESC', 'first'),PARTICIPANTES = ('PARTICIPANTES', 'first'),
                                           ESTADO = ('SG_UF_ESC', 'first')).reset_index().sort_values('MEDIAS_OBJETIVA', ascending = False)
    # Insere o nome das escolas usando o Censo Escolar, se o codInep não constar no Censo, deixa o nome vazio
    geral = geral.merge(escolas, on='CO_ESCOLA', how='left')
    geral.fillna(value="", inplace=True)
    # Arredonda as notas pra duas casas decimais, muda a ordem e renomeia as colunas
    geral[['CN', 'CH', 'LC', 'MT', 'MEDIAS_OBJETIVA', 'RED',  'OBJETIVAS_COM_RED']] = geral[['CN', 'CH', 'LC', 'MT', 'MEDIAS_OBJETIVA', 'RED',  'OBJETIVAS_COM_RED']].round(2)
    geral = geral[['CO_ESCOLA', 'NOME', 'MUNICIPIO','ESTADO', 'PARTICIPANTES', 'CH', 'CN', 'LC', 'MT', 'MEDIAS_OBJETIVA', 'RED',  'OBJETIVAS_COM_RED' ]]
    geral.rename(columns= {"CO_ESCOLA": "codInep",
                           "NOME": "nome",
                           "MUNICIPIO": "municipio",
                           "ESTADO": "estado",
                           "PARTICIPANTES": "participantes",
                           "CH": "mediaCH","CN": "mediaCN","LC": "mediaLC","MT": "mediaMT","MEDIAS_OBJETIVA": "mediaObj","RED": "mediaRed","OBJETIVAS_COM_RED":  "mediaGeral"}, inplace=True)
    return geral

# Filtro: Notas objetivas não nulas, possui Cod Escola, concluindo o ensino médio. Adiciona colunas com médias objetiva e com redação
dados_filtrado = dados[(dados.NU_NOTA_CH > 0) &(dados.NU_NOTA_LC > 0) &(dados.NU_NOTA_MT > 0) &(dados.NU_NOTA_CN > 0) &(dados.CO_ESCOLA > 0) & (dados.TP_ST_CONCLUSAO == 2) & (dados.TP_ENSINO == 1)]
dados_filtrado = dados_filtrado.assign(OBJETIVA = (dados_filtrado.NU_NOTA_CH + dados_filtrado.NU_NOTA_CN + dados_filtrado.NU_NOTA_LC + dados_filtrado.NU_NOTA_MT)/4)
dados_filtrado.fillna({'NU_NOTA_REDACAO' : 0}, inplace=True)
dados_filtrado = dados_filtrado.assign(MEDIA_RD = (dados_filtrado.NU_NOTA_CH + dados_filtrado.NU_NOTA_CN + dados_filtrado.NU_NOTA_LC + dados_filtrado.NU_NOTA_MT + dados_filtrado.NU_NOTA_REDACAO)/5)
# Adiciona número de participantes da escola do aluno
dados_filtrado = dados_filtrado.merge(dados_filtrado.groupby('CO_ESCOLA').agg(PARTICIPANTES = ('NU_INSCRICAO', 'count')).reset_index())
df = medias_escolas(dados_filtrado, 0)
nomeJson = f'mediasEnem_{ano}.json'
df.to_json(nomeJson, orient='records')

# Salva num banco MongoDB local
#!mongoimport --db=apiNotasEnem --collection=2019 --type=json --file={nomeJson} --jsonArray --drop
# Criar index na variável `codInep` posteriormente para melhor performace