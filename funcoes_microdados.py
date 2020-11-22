import pandas as pd
def ler_microdados(ano, caminho_arquivo):
    r"""
        Lê o arquivo dos microdados do ENEM de 2009 até 2019.
        Retorna um DataFrame sempre com as mesmas colunas. (Testado com todos os arquivos baixados em 22/11/2020)

        Os arquivos de diferentes anos não tem um mesmo padrão de extensão, separador e nome das colunas, esse script leva tudo isso em consideração.
        Atualmente, 2011 é único arquivo que não é CSV, é TXT, para ler sem ocupar uma enorme quantidade de memória ram, ele é lido em `chunks` e isso faz com que demore mais.
        Os arquivos podem ser atualizados pelo INEP a qualquer momento.
        
        Parâmetros:
        ----------
        ano: int, str
            ano do arquivo. Se for string deve conter somente os 4 dígitos do ano.
        caminho : str
            Caminho do arquivo.
        
        Retorna:
        -------
        DataFrame com 12 colunas:
            NU_INSCRICAO => id do participante
            TP_ST_CONCLUSAO => situação de conclusão do participante
            TP_ENSINO => tipo de ensino do participante
            CO_ESCOLA => código INEP da escola
            NO_MUNICIPIO_ESC => nome do município da escola
            SG_UF_ESC => sigla do estado da escola
            TP_DEPENDENCIA_ADM_ESC => dependência administrativa da escola
            NU_NOTA_CN => nota de Ciências da Natureza do participante
            NU_NOTA_CH => nota de Ciências Humanas do participante
            NU_NOTA_LC => nota de Linguagens e Códigos do participante
            NU_NOTA_MT => nota de Matemática do participante
            NU_NOTA_REDACAO => nota de Redação do participante
    """
    
    separador = ';'
    colunas_str = 'NU_INSCRICAO,CO_ESCOLA,NO_MUNICIPIO_ESC,SG_UF_ESC,TP_DEPENDENCIA_ADM_ESC,TP_ST_CONCLUSAO,TP_ENSINO,NU_NOTA_CH,NU_NOTA_CN,NU_NOTA_LC,NU_NOTA_MT,NU_NOTA_REDACAO'
    inverse_mapping_cols = {}
    
    if (int(ano) == 2015): separador = ','

    if (int(ano) == 2014 or int(ano) == 2013):
        if (int(ano) == 2014): separador = ','
        # Mudança nos nomes de algumas colunas
        mapping_cols = {
        'CO_ESCOLA' : 'COD_ESCOLA',
        'SG_UF_ESC' : 'UF_ESC',
        'TP_DEPENDENCIA_ADM_ESC' : 'ID_DEPENDENCIA_ADM_ESC',
        'TP_ST_CONCLUSAO' : 'ST_CONCLUSAO',
        'TP_ENSINO' : 'IN_TP_ENSINO',
        'NU_NOTA_CH' : 'NOTA_CH',
        'NU_NOTA_CN' : 'NOTA_CN',
        'NU_NOTA_LC' : 'NOTA_LC',
        'NU_NOTA_MT' : 'NOTA_MT',
        }
        # mapping invertido para renomear as colunas após ler o CSV. Assim é possível padronizar o restante do script
        inverse_mapping_cols = {v : k for k, v in mapping_cols.items()}
        for col in mapping_cols:
            colunas_str = colunas_str.replace(col, mapping_cols[col])

    if (int(ano) == 2012 or int(ano) == 2011):
        separador = ','
        # Mudança nos nomes de algumas colunas
        mapping_cols = {
        'CO_ESCOLA' : 'PK_COD_ENTIDADE',
        'SG_UF_ESC' : 'UF_ESC',
        'TP_DEPENDENCIA_ADM_ESC' : 'ID_DEPENDENCIA_ADM',
        'TP_ST_CONCLUSAO' : 'ST_CONCLUSAO',
        'TP_ENSINO' : 'IN_TP_ENSINO',
        'NU_NOTA_CH' : 'NU_NT_CH',
        'NU_NOTA_CN' : 'NU_NT_CN',
        'NU_NOTA_LC' : 'NU_NT_LC',
        'NU_NOTA_MT' : 'NU_NT_MT',
        }
        # mapping invertido para renomear as colunas após ler o CSV. Assim é possível padronizar o restante do script
        inverse_mapping_cols = {v : k for k, v in mapping_cols.items()}
        for col in mapping_cols:
            colunas_str = colunas_str.replace(col, mapping_cols[col])
        # 2011 é TXT, ler em pedaços e pegar só as colunas de interesse para não ocupar muita memória ram
        if (int(ano) == 2011):
            print("Lendo microdados do ENEM 2011. O arquivo desse ano é FWF(fixed-width formatted) e não CSV, por isso a leitura demora mais tempo.")
            df_chunk = pd.DataFrame()
            for chunk in pd.read_fwf(caminho_arquivo.replace('.csv', '.txt'), chunksize=1000000, encoding='latin', na_values='.', widths=[12,4,3,1,7,150,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,8,7,150,2,1,1,1,7,150,2,1,1,1,1,9,9,9,9,45,45,45,45,3,3,3,3,1,45,45,50,45,1,9,9,9,9,9,9,1,2,8,7,150,2,1,1,1], names=["NU_INSCRICAO","NU_ANO","IDADE","TP_SEXO","COD_MUNICIPIO_INSC","NO_MUNICIPIO_INSC","UF_INSC","ST_CONCLUSAO","IN_TP_ENSINO","IN_CERTIFICADO","IN_BRAILLE","IN_AMPLIADA","IN_LEDOR","IN_ACESSO","IN_TRANSCRICAO","IN_LIBRAS","IN_UNIDADE_PRISIONAL","IN_BAIXA_VISAO","IN_CEGUEIRA","IN_DEFICIENCIA_AUDITIVA","IN_DEFICIENCIA_FISICA","IN_DEFICIENCIA_MENTAL","IN_DEFICIT_ATENCAO","IN_DISLEXIA","IN_GESTANTE","IN_LACTANTE","IN_LEITURA_LABIAL","IN_SABATISTA","IN_SURDEZ","TP_ESTADO_CIVIL","TP_COR_RACA","PK_COD_ENTIDADE","COD_MUNICIPIO_ESC","NO_MUNICIPIO_ESC","UF_ESC","ID_DEPENDENCIA_ADM","ID_LOCALIZACAO","SIT_FUNC","COD_MUNICIPIO_PROVA","NO_MUNICIPIO_PROVA","UF_MUNICIPIO_PROVA","IN_PRESENCA_CN","IN_PRESENCA_CH","IN_PRESENCA_LC","IN_PRESENCA_MT","NU_NT_CN","NU_NT_CH","NU_NT_LC","NU_NT_MT","TX_RESPOSTAS_CN","TX_RESPOSTAS_CH","TX_RESPOSTAS_LC","TX_RESPOSTAS_MT","ID_PROVA_CN","ID_PROVA_CH","ID_PROVA_LC","ID_PROVA_MT","TP_LINGUA","DS_GABARITO_CN","DS_GABARITO_CH","DS_GABARITO_LC","DS_GABARITO_MT","IN_STATUS_REDACAO","NU_NOTA_COMP1","NU_NOTA_COMP2","NU_NOTA_COMP3","NU_NOTA_COMP4","NU_NOTA_COMP5","NU_NOTA_REDACAO","IN_CONCLUINTE_CENSO","COD_ETAPA_ENSINO_CENSO","COD_ENTIDADE_CENSO","COD_MUNICIPIO_ESC_CENSO","NO_MUNICIPIO_ESC_CENSO","UF_ESC_CENSO","ID_DEPENDENCIA_ADM_CENSO","ID_LOCALIZACAO_CENSO","SIT_FUNC_CENSO"]):
                df_chunk = pd.concat([df_chunk, chunk[["NU_INSCRICAO","ST_CONCLUSAO","IN_TP_ENSINO","PK_COD_ENTIDADE","NO_MUNICIPIO_ESC","UF_ESC","ID_DEPENDENCIA_ADM","NU_NT_CN","NU_NT_CH","NU_NT_LC","NU_NT_MT","NU_NOTA_REDACAO"]]])
            return df_chunk.rename(columns=inverse_mapping_cols)

    colunas_vetor = colunas_str.split(',')
    # Especifica tipos de dados para menor consumo de memória, não é necessário, mas economiza cerca de 100 Mb
    dic_types = {}
    for i in range(len(colunas_vetor)):
        if i==0: dic_types[colunas_vetor[i]] = 'Int64'
        if i==1: dic_types[colunas_vetor[i]] = 'Int32'
        if i==4: dic_types[colunas_vetor[i]] = 'Int8'
        if i==5: dic_types[colunas_vetor[i]] = 'Int8'
        if i==6: dic_types[colunas_vetor[i]] = 'Int8'
        if i==11: dic_types[colunas_vetor[i]] = 'Int16'
    microdados = pd.read_csv(
        caminho_arquivo,
        sep=separador,
        encoding='latin',
        usecols=colunas_vetor,
        na_values='.',
        dtype=dic_types
    ).rename(columns=inverse_mapping_cols)
    return microdados

def ler_escolas_censo(ano, caminho_arquivo):
    r"""
        Lê a tabela de Escolas do Censo escolar de 2009 até 2019.
    
        Parâmetros:
        ----------
        ano: int, str
            ano do arquivo. Se for string deve conter somente os 4 dígitos do ano.
        caminho : str
            Caminho do arquivo.

        Retorna:
        -------
        DataFrame com 2 colunas:
            CO_ESCOLA => código INEP da escola
            NOME => nome da escola
    """
    colunas_escola_censo = ['CO_ENTIDADE', 'NO_ENTIDADE']
    dic_rename_censo = {'CO_ENTIDADE':'CO_ESCOLA', 'NO_ENTIDADE':'NOME'}
    if (int(ano) == 2014 or int(ano) == 2013 or int(ano) == 2012 or int(ano) == 2011 or int(ano) == 2010 or int(ano) == 2009):
        colunas_escola_censo = ['PK_COD_ENTIDADE', 'NO_ENTIDADE']
        dic_rename_censo = {'PK_COD_ENTIDADE':'CO_ESCOLA', 'NO_ENTIDADE':'NOME'}
    return pd.read_csv(caminho_arquivo, usecols=colunas_escola_censo,sep='|', encoding='latin').rename(columns=dic_rename_censo)