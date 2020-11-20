# mediasENEM
Script calcula as notas médias por escola do ENEM 2019.  
Os filtros usados nos participantes são:  
  * Cada nota objetiva não nula (NU_NOTA_CH > 0, NU_NOTA_LC > 0, NU_NOTA_MT > 0, NU_NOTA_CN > 0).
  * Variável CO_ESCOLA existente.
  * Concluintes no ensino médio (TP_ST_CONCLUSAO == 2).
  * Tipo de ensino regular (TP_ENSINO == 1).

Os dados do Censo Escolar(é usado somente para o nome das escolas), e os microdados de notas do ENEM podem ser encontrados no link do [INEP](http://inep.gov.br/microdados/)
