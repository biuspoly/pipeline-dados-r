library(tidyr)
library(dplyr)
library(glue)
library(parallel)
library(future)
library(future.apply)
library(lubridate)
library(stringi)
library(purrr)
library(httr)
library(jsonlite)


# capture.output(type = "message", file = "dados brutos/atualiza_dados_brutos_log.txt", {

  # Configurações do banco --------------------------------------------------
  # Versão pública: ERP e MySQL substituídos por Parquet (ver R/funcoes_pipeline.R)


  empresas = data.frame()

  empresas <- rbind(empresas, data.frame(cnpj="11222333000101", Nome="Alfa", banco="ERP_ALFA", digito=1, tabela_prod=1, prefixo = "", transferencia = ""))
  empresas <- rbind(empresas, data.frame(cnpj="11222444000102", Nome="Beta Leste", banco="ERP_BETA_LESTE", digito=1, tabela_prod=1, prefixo = "", transferencia = I(list(c(900001)))))
  empresas <- rbind(empresas, data.frame(cnpj="11222444000203", Nome="Beta Oeste", banco="ERP_BETA_OESTE", digito=2, tabela_prod=1, prefixo = "", transferencia = I(list(c(900002)))))
  empresas <- rbind(empresas, data.frame(cnpj="11222555000104", Nome="Gama Norte", banco="ERP_GAMA_NORTE", digito=1, tabela_prod=1, prefixo = "", transferencia = ""))
  empresas <- rbind(empresas, data.frame(cnpj="11222666000105", Nome="Delta", banco="ERP_DELTA", digito=1, tabela_prod=1, prefixo = "", transferencia = ""))
  # empresas <- rbind(empresas, data.frame(cnpj="11222777000206", Nome="Epsilon", banco="ERP_EPSILON", digito=2, tabela_prod=1, prefixo = "ERP_EPSILON_AUX.", transferencia = ""))
  # empresas <- rbind(empresas, data.frame(cnpj="11222888000107", Nome="Gama Sul", banco="ERP_GAMA_SUL", digito=1, tabela_prod=1, prefixo = "", transferencia = ""))
  # empresas <- rbind(empresas, data.frame(cnpj="11222888000208", Nome="Gama Centro", banco="ERP_GAMA_CENTRO", digito=2, tabela_prod=2, prefixo = "", transferencia = ""))
  # empresas <- rbind(empresas, data.frame(cnpj="11222999000209", Nome="Zeta Polo 1", banco="ERP_ZETA_1", digito=2, tabela_prod=1, prefixo = "", transferencia = ""))
  # empresas <- rbind(empresas, data.frame(cnpj="11222999000310", Nome="Zeta Polo 2", banco="ERP_ZETA_2", digito=3, tabela_prod=1, prefixo = "", transferencia = ""))
  # empresas <- rbind(empresas, data.frame(cnpj="11222999000111", Nome="Zeta Polo 3", banco="ERP_ZETA_3", digito=1, tabela_prod=1, prefixo = "", transferencia = ""))

  # clientes_completo = data.frame()

  # plan(multisession, workers = as.integer(detectCores()/1.25))
  plan(sequential)

  registros_dev = future_lapply(1:nrow(empresas), function(linha_empresa) {

    filtro_data = format(Sys.Date() %m-% months(6),'%Y%m')


    # for (linha_empresa in 1:nrow(empresas)){

    # url <- glue("https://brasilapi.com.br/api/cnpj/v1/{empresas$cnpj[linha_empresa]}")
    # res <- GET(url)
    #
    # if (status_code(res) == 200) {
    #   dados <- content(res, as = "text", encoding = "UTF-8")
    #   dados_list <- fromJSON(dados, flatten = TRUE)
    #
    #   # Selecionar apenas os campos simples, ignorando listas complexas
    #   revendas = data.frame(razao_social = dados_list$razao_social, logradouro = dados_list$logradouro,
    #                         bairro = dados_list$bairro, uf = dados_list$uf, cep = dados_list$cep,
    #                         numero = dados_list$numero, municipio = dados_list$municipio)
    #
    # }

    # con = dbConnect(odbc(), dsn = empresas$banco[linha_empresa], encoding = "UTF-8", charset = "utf8mb4")
    banco = empresas$banco[linha_empresa]
    print(glue("Conectou na empresa {empresas$Nome[linha_empresa]}"))

    clientes = consulta_erp(banco, "clientes", glue("
  select
      SUBSTRING(CAST(CLI_CNPJ_CPF AS VARCHAR(20)),LENGTH(CAST(CLI_CNPJ_CPF AS VARCHAR(20)))-5,4) AS CGC_CPF,
      REPLACE(CLI_FANTASIA,'Ç','C') AS FANTASIA,
      CLI_CNPJ_CPF AS 'CHAVE',
      SUBSTRING(CAT_NOME,1,13) AS 'CANAL',
      CAT_CODIGO AS COD_CATEGORIA,
      SUBSTRING(CLI_MUNICIPIO_COB,1,15) AS CIDADE,
      CAST(CLI_REGIAO AS VARCHAR(4)) || RIGHT('000' || LTRIM(RTRIM(CAST(CLI_NUMERO AS VARCHAR(4)))),4) AS 'CODCLI' ,
      CASE
          WHEN CLI_PORTE = 'D' THEN 'Diamante'
          WHEN CLI_PORTE = 'O' THEN 'Ouro'
          WHEN CLI_PORTE = 'P' THEN 'Prata'
          WHEN CLI_PORTE = 'B' THEN 'Bronze' ELSE 'S/Definicao'
      END  AS PORTE,
      CASE
          WHEN CLI_PORTE = 'D' THEN 2.5
          WHEN CLI_PORTE = 'O' THEN 2
          WHEN CLI_PORTE = 'P' THEN 1.7
          WHEN CLI_PORTE = 'B' THEN 1.4 ELSE 1.4
      END  AS ACRESCIMO,
      CLI_VENDEDOR AS VEN,
      REPLACE(SUBSTRING(CLI_RAZAO_SOCIAL,1,30),'Ç','C') AS NOME,
      CLI_TIPO_COBRANCA AS TPCOBRANCA,
      CLI_COND_PAGTO AS CPG,
      CLI_BANCO AS BANCO,
      CLI_LIMITE_CREDITO AS 'LIMITE_CREDITO',
      (RTRIM(CLI_END_TIPO) || ' ' || RTRIM(CLI_ENDERECO)|| ', ' || RTRIM(CAST(CLI_END_NUMERO AS VARCHAR(4))) || ' - ' || RTRIM(CLI_BAIRRO) || ' - ' || RTRIM(CLI_MUNICIPIO) || ' - ' || RTRIM(CLI_UF)) AS ENDERECO,
      RTRIM(CLI_CONTATO) AS CONTATO,
      RTRIM(CLI_PONTO_REF) AS PTO_REF,
      RTRIM(CAST(CLI_DIA_VISITA AS VARCHAR(4))) AS DIA_VISIT,
      RTRIM(CAST(CLI_SEQ_VISITA AS VARCHAR(4))) AS SEQ,
      RTRIM(CAST(CLI_CEP AS VARCHAR(10))) AS CEP,
      RTRIM(CAST(CLI_FONE AS VARCHAR(20))) AS FONE,
      CLI_PROPRIETARIO AS PROPRIETARIO,
      CLI_CPF_RESP AS CPF_PROP,
      CLI_NASC_PROP AS NASC_PROP,
      CLI_EMAIL AS EMAIL_PROP,
      CLI_DIA_VISITA AS QUINZENAL

      FROM {empresas$prefixo[linha_empresa]}ERP_CLIENTES
      INNER JOIN {empresas$prefixo[linha_empresa]}ERP_CATEGORIAS_CLIENTE ON CAT_CODIGO = CLI_CATEGORIA

  "))

    produtos = consulta_erp(banco, "produtos",

                          if(empresas$Nome[linha_empresa] == "Epsilon"){
                            glue("
        SELECT
          RIGHT('0000'||CAST(PRD_FAMILIA AS VARCHAR(3)),3) || RIGHT('0000'||CAST(PRD_NUMERO AS VARCHAR(3)),3) AS CODCOMP,
          PRD_COD_REDUZIDO AS CODRED,
          PRD_DESCRICAO AS PRODUTO,
          RTRIM(PRD_DESCRICAO_GENERICA) AS PRODUTO_GENERICO,
          PRD_QTD_UNIDADE AS UNIDADE,
          '' as TIPO,
          PRD_COMODATO AS CEV

          FROM {empresas$prefixo[linha_empresa]}ERP_PRODUTOS_0{empresas$tabela_prod[linha_empresa]} P

        ")
                          }
                          else{
                            glue("
        SELECT
          RIGHT('0000'||CAST(PRD_FAMILIA AS VARCHAR(3)),3) || RIGHT('0000'||CAST(PRD_NUMERO AS VARCHAR(3)),3) AS CODCOMP,
          PRD_COD_REDUZIDO AS CODRED,
          PRD_DESCRICAO AS PRODUTO,
          RTRIM(PRD_DESCRICAO_GENERICA) AS PRODUTO_GENERICO,
          PRD_QTD_UNIDADE AS UNIDADE,
          M.GRP_DESCRICAO TIPO,
          PRD_COMODATO AS CEV

          FROM {empresas$prefixo[linha_empresa]}ERP_PRODUTOS_0{empresas$tabela_prod[linha_empresa]} P
          LEFT JOIN {empresas$prefixo[linha_empresa]}ERP_GRUPOS_PRODUTO_0{empresas$tabela_prod[linha_empresa]} M ON M.GRP_CODIGO = P.PRD_GRUPO

        ")
                          }
    )

    preco_prod = consulta_erp(banco, "preco_prod", glue("
  SELECT
  RIGHT('0000'||CAST(TAB_FAMILIA AS VARCHAR(3)),3) || RIGHT('0000'||CAST(TAB_NUMERO AS VARCHAR(3)),3) AS CODCOMP,
  TAB_PRECO_UN AS PRECO_UN,
  TAB_PRECO AS PRECO,
  CAST(TAB_ANO AS VARCHAR(4)) || RIGHT('00' || CAST(TAB_MES AS VARCHAR(2)), 2) || CAST(TAB_NUM_TABELA AS VARCHAR(2)) AS TAB_CEV
  FROM {empresas$prefixo[linha_empresa]}ERP_TABELA_PRECOS_0{empresas$tabela_prod[linha_empresa]}
  WHERE
    TAB_NUM_TABELA = 99
    AND TAB_VIGENCIA_INI <= {format(Sys.Date(), format = '%Y%m%d')}
    AND TAB_VIGENCIA_FIM >= {format(Sys.Date(), format = '%Y%m%d')}
  "))

    vendas_e_dev = consulta_erp(banco, "vendas_e_dev", glue("
  SELECT  PED_NUMERO AS COD_PED,
          ITE_COD_PRODUTO AS CODR,
          ITE_VENDEDOR AS VEN,PED_MOTORISTA AS mot,
          ITE_CLIENTE AS CLI,
          D.MDV_DESCRICAO,
          CMP_TIPO,
          CASE
             WHEN PED_TIPO_COBRANCA = 4 THEN 'Prazo'
             WHEN PED_TIPO_COBRANCA = 1 THEN 'A Vista'
             ELSE 'OUTRO' END AS TPCOBRA,
          PED_NF NF_VENDA,
          PED_ROMANEIO AS ROMANEIO,
          SUM(ITE_QTD) AS 'QTD',
          SUM(ITE_QTD_DEVOLVIDA) DEVOLUCAO_CX,
          SUM(ITE_QTD_UN) QTD_UN,
          SUM(ITE_QTD_UN_DEVOLVIDA) AS DEVOLUCAO_UN,
          SUM( CASE
  	WHEN ITE_OCORRENCIA IN (1,60) THEN ITE_VALOR_TOTAL
            ELSE 0 END) AS 'valor venda',
          SUM( CASE
  	WHEN ITE_OCORRENCIA IN (1,60) THEN ITE_VALOR_DEVOLVIDO
            ELSE 0 END) AS 'valor devolucao',
          SUBSTRING(CAST(ITE_NUM_PEDIDO AS VARCHAR(255)), 1, 6) AS MES,
          SUBSTRING(CAST(ITE_NUM_PEDIDO AS VARCHAR(255)), 1, 8) AS DATA_VENDA,
          --SUBSTRING(CAST(PED_NUMERO AS VARCHAR(255)), 1, 8) AS DATA_PEDIDO,
          PED_DT_NF_DEVOLUCAO as DATA_DEV,
          ITE_NF_DEVOLUCAO as NF_DEV
  FROM {empresas$prefixo[linha_empresa]}ERP_PEDIDO_ITENS_0{empresas$digito[linha_empresa]}
  inner JOIN {empresas$prefixo[linha_empresa]}ERP_PEDIDOS_0{empresas$digito[linha_empresa]} C ON ITE_NUM_PEDIDO = PED_NUMERO
  LEFT JOIN {empresas$prefixo[linha_empresa]}ERP_MOTIVOS_DEVOLUCAO D ON D.MDV_CODIGO = PED_MOTIVO_DEV
  left join {empresas$prefixo[linha_empresa]}ERP_COMODATO_PENDENTE_0{empresas$digito[linha_empresa]} on CMP_PEDIDO = PED_NUMERO
  --LEFT JOIN ERP_FISCAL_ENTRADAS_2501 ON FEN_NF = PED_NF
  WHERE ITE_NUM_PEDIDO > {filtro_data}000000
    AND (PED_SITUACAO = 8 OR PED_SITUACAO = 0)   -- CAPA
    AND ITE_SITUACAO <> 'C'                     -- ITEM
    AND ITE_COD_PRODUTO <> 0
    AND ITE_OCORRENCIA IN (1, 2, 60, 50)
  GROUP BY ITE_NUM_PEDIDO, PED_NUMERO,ITE_COD_PRODUTO, MES, CLI, VEN,D.MDV_DESCRICAO, mot, NF_VENDA, DATA_DEV, NF_DEV, ROMANEIO, CMP_TIPO, TPCOBRA
  "))

    motoristas = consulta_erp(banco, "motoristas", glue("
  select 	v.VEN_CODIGO COD,
        	v.VEN_NOME NOME
        from {empresas$prefixo[linha_empresa]}ERP_VENDEDORES_0{empresas$digito[linha_empresa]} v where v.VEN_TIPO='M'
  "))

    if(empresas$Nome[linha_empresa] == "Zeta Polo 1"){
      vendedores = consulta_erp(banco, "vendedores", glue("
        select 	v.VEN_CODIGO COD,
              	v.VEN_NOME NOME
        from {empresas$prefixo[linha_empresa]}ERP_VENDEDORES_0{empresas$digito[linha_empresa]} v
        where v.VEN_TIPO='V' and
              CAST(v.VEN_CODIGO AS INTEGER) not between 400 and 499
  "))
    }
    else{
      vendedores = consulta_erp(banco, "vendedores", glue("
        select 	v.VEN_CODIGO COD,
              	v.VEN_NOME NOME
        from {empresas$prefixo[linha_empresa]}ERP_VENDEDORES_0{empresas$digito[linha_empresa]} v
        where v.VEN_TIPO='V'
  "))
    }

    romaneios = consulta_erp(banco, "romaneios", glue("
  SELECT
        CAST(ROM_ANO AS VARCHAR(4)) ||
        RIGHT('00' || LTRIM(RTRIM(CAST(ROM_MES AS VARCHAR(2)))), 2) ||
        RIGHT('00000' || LTRIM(RTRIM(CAST(ROM_SEQ AS VARCHAR(5)))), 5) AS ROMANEIO,
        ROM_DT_BAIXA AS DATA,
        ROM_MOTORISTA AS MOTORISTA
  FROM {empresas$prefixo[linha_empresa]}ERP_ROMANEIOS_0{empresas$digito[linha_empresa]}
  WHERE CAST(ROM_ANO AS VARCHAR(4)) ||
          RIGHT('00' || LTRIM(RTRIM(CAST(ROM_MES AS VARCHAR(2)))), 2) ||
          RIGHT('00000' || LTRIM(RTRIM(CAST(ROM_SEQ AS VARCHAR(5)))), 5) >= '20250101000'
  "))


    vend_dev_itens = consulta_erp(banco, "vend_dev_itens", glue("
  SELECT
    ped.ITE_NUM_PEDIDO as cod_ped,
    cad.PRD_COD_REDUZIDO AS codr,
    cad.PRD_DESCRICAO_GENERICA,
    ped.ITE_CFOP as cfop,
  SUM(ped.ITE_QTD) AS 'QTD',
  SUM(ped.ITE_QTD_DEVOLVIDA) DEVOLUCAO_CX,
  SUM( CASE
  	WHEN ped.ITE_OCORRENCIA IN (1,60) THEN ped.ITE_VALOR_TOTAL
            ELSE 0 END) AS 'valor venda',
          SUM( CASE
  	WHEN ped.ITE_OCORRENCIA IN (1,60) THEN ped.ITE_VALOR_DEVOLVIDO
            ELSE 0 END) AS 'valor devolucao'
  FROM
    {empresas$prefixo[linha_empresa]}ERP_PRODUTOS_0{empresas$tabela_prod[linha_empresa]} AS cad
  INNER JOIN
    {empresas$prefixo[linha_empresa]}ERP_PEDIDO_ITENS_0{empresas$digito[linha_empresa]} AS ped
    ON cad.PRD_COD_REDUZIDO = ped.ITE_COD_PRODUTO
  WHERE
    ped.ITE_NUM_PEDIDO >= {filtro_data}000000
  GROUP BY ped.ITE_NUM_PEDIDO, cad.PRD_COD_REDUZIDO, cad.PRD_DESCRICAO_GENERICA, ped.ITE_CFOP

  "))

    pedidos = consulta_erp(banco, "pedidos", glue("
  SELECT DISTINCT
        PED_NUMERO AS PEDIDO,
        PED_DT_EMISSAO AS DT,
        PED_ROMANEIO AS ROMANEIO,
        PED_CLIENTE AS CODCLI,
        PED_VENDEDOR AS COD_VENDEDOR,
        PED_TIPO_COBRANCA AS TIPO_COBRANCA,
        ITE_OCORRENCIA AS OCO,
        CASE WHEN PED_ORIGEM_COD = 'D' THEN 'ERP'
             WHEN PED_ORIGEM_COD = 'L' THEN 'Portal'
             WHEN PED_ORIGEM_COD = 'P' THEN 'SFA'
             WHEN PED_ORIGEM_COD = 'G' THEN 'Loja Parceira'
             WHEN PED_ORIGEM_COD = 'R' THEN 'SFA'
             ELSE PED_ORIGEM_COD
        END AS PED_ORIGEM,
        PED_NF AS NOTA_FISCAL,
        PED_VALOR AS VALOR,
        NFE_CHAVE AS CHAVE_NFE,
        PED_VENCIMENTO AS VENC,
        PED_SITUACAO AS STATUS,
        PED_FORA_ROTA AS FORA_ROTA,
        PED_USUARIO AS REPROGRAMADO,
        SUBSTRING(CAST(BXA_DT_PAGAMENTO AS VARCHAR(8)), 1,4) || SUBSTRING(CAST(BXA_DT_PAGAMENTO AS VARCHAR(8)), 5,2) || SUBSTRING(CAST(BXA_DT_PAGAMENTO AS VARCHAR(8)), 7,2) AS PAG

        FROM {empresas$prefixo[linha_empresa]}ERP_PEDIDOS_0{empresas$digito[linha_empresa]}
        LEFT JOIN {empresas$prefixo[linha_empresa]}ERP_BAIXAS_0{empresas$digito[linha_empresa]} ON BXA_PEDIDO = PED_NUMERO
        LEFT JOIN {empresas$prefixo[linha_empresa]}ERP_NOTAS_FISCAIS_0{empresas$digito[linha_empresa]} ON NFE_NUMERO = PED_NF
        LEFT JOIN {empresas$prefixo[linha_empresa]}ERP_PEDIDO_ITENS_0{empresas$digito[linha_empresa]} ON ITE_NUM_PEDIDO = PED_NUMERO
        WHERE  CAST(RTRIM(CAST(PED_ROMANEIO AS VARCHAR(6))) AS INTEGER) >= {filtro_data} OR
               PED_ROMANEIO = 0
  "))

    # on.exit(dbDisconnect(con))

    if (nrow(clientes) > 0){
      clientes$CNPJ_REV = empresas$cnpj[linha_empresa]
    }
    if (nrow(produtos) > 0){
      produtos$CNPJ_REV = empresas$cnpj[linha_empresa]
    }
    if (nrow(vendas_e_dev) > 0){
      vendas_e_dev$CNPJ_REV = empresas$cnpj[linha_empresa]
    }
    if (nrow(motoristas) > 0){
      motoristas$CNPJ_REV = empresas$cnpj[linha_empresa]
    }
    if (nrow(vendedores) > 0){
      vendedores$CNPJ_REV = empresas$cnpj[linha_empresa]
    }
    if (nrow(romaneios) > 0){
      romaneios$CNPJ_REV = empresas$cnpj[linha_empresa]
    }
    if (nrow(vend_dev_itens) > 0){
      vend_dev_itens$CNPJ_REV = empresas$cnpj[linha_empresa]
    }
    # if (nrow(revendas) > 0){
    #   revendas$CNPJ_REV = empresas$cnpj[linha_empresa]
    # }
    if (nrow(preco_prod) > 0){
      preco_prod$CNPJ_REV = empresas$cnpj[linha_empresa]
    }
    if (nrow(pedidos) > 0){
      pedidos$CNPJ_REV = empresas$cnpj[linha_empresa]
    }


    return(list(clientes,
                produtos,
                vendas_e_dev,
                motoristas,
                vendedores,
                romaneios,
                vend_dev_itens,
                preco_prod,
                pedidos))
  }, future.seed = NULL)

  clientes_completo = map_dfr(registros_dev, 1)
  produtos_completo = map_dfr(registros_dev, 2)
  vendas_e_dev_completo = map_dfr(registros_dev, 3)
  motoristas_completo = map_dfr(registros_dev, 4)
  vendedores_completo = map_dfr(registros_dev, 5)
  romaneios_completo = map_dfr(registros_dev, 6)
  vend_dev_itens_completo = map_dfr(registros_dev, 7)
  # revendas_completo = map_dfr(registros_dev, 8)
  preco_completo = map_dfr(registros_dev, 8)
  pedidos_completo = map_dfr(registros_dev, 9)

  ## Correcao, agora Polo 2 é junto com Polo 1
  vendedores_completo$CNPJ_REV[((as.numeric(vendedores_completo$COD) >= 400 &
                                   as.numeric(vendedores_completo$COD) <= 499) |
                                  (as.numeric(vendedores_completo$COD) >= 742 &
                                     as.numeric(vendedores_completo$COD) <= 743)) &
                                 vendedores_completo$CNPJ_REV == "11222999000209"] <- "11222999000310"
  vendas_e_dev_completo$CNPJ_REV[((as.numeric(vendas_e_dev_completo$VEN) >= 400 &
                                     as.numeric(vendas_e_dev_completo$VEN) <= 499) |
                                    (as.numeric(vendas_e_dev_completo$VEN) >= 742 &
                                       as.numeric(vendas_e_dev_completo$VEN) <= 743)) &
                                   vendas_e_dev_completo$CNPJ_REV == "11222999000209" &
                                   vendas_e_dev_completo$COD_PED >202509000000] <- "11222999000310"


  # Dados brutos ------------------------------------------------------------
  # No servidor: dbConnect(RMariaDB::MariaDB(), ...) na base da camada bruta.
  # Aqui cada tabela vira dados/bruto/<tabela>.parquet

  if (nrow(clientes_completo)>0){

    # cria a tabela com os dados do dataframe
    clientes_completo <- clientes_completo %>%
      mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
    # apaga a tabela se já existir e grava
    gravar_tabela("bruto", "tb_clientes", clientes_completo)

    # particoes = 2000
    # total_inserted <- 0
    # for (particao in seq(1, nrow(clientes_completo), by = particoes)) {
    #   dbAppendTable(con, "tb_clientes", clientes_completo[particao:min(particao + particoes - 1, nrow(clientes_completo)), ])
    #   total_inserted <- total_inserted + nrow(clientes_completo[particao:min(particao + particoes - 1, nrow(clientes_completo)), ])
    # }

  }

  if (nrow(produtos_completo)>0){

    # cria a tabela com os dados do dataframe
    produtos_completo <- produtos_completo %>%
      mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
    gravar_tabela("bruto", "tb_produtos", produtos_completo)

  }

  if (nrow(motoristas_completo)>0){

    # cria a tabela com os dados do dataframe
    motoristas_completo <- motoristas_completo %>%
      mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
    gravar_tabela("bruto", "tb_motorista", motoristas_completo)

  }

  if (nrow(vendedores_completo)>0){

    # cria a tabela com os dados do dataframe
    vendedores_completo <- vendedores_completo %>%
      mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
    gravar_tabela("bruto", "tb_vendedor", vendedores_completo)

  }


  if (nrow(vendas_e_dev_completo)>0){

    # cria a tabela com os dados do dataframe
    vendas_e_dev_completo <- vendas_e_dev_completo %>%
      mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
    gravar_tabela("bruto", "tb_vendas_e_dev", vendas_e_dev_completo)

  }

  if (nrow(romaneios_completo)>0){

    # cria a tabela com os dados do dataframe
    romaneios_completo <- romaneios_completo %>%
      mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
    gravar_tabela("bruto", "tb_rom_dev", romaneios_completo)

  }

  if (nrow(vend_dev_itens_completo)>0){

    # cria a tabela com os dados do dataframe
    vend_dev_itens_completo <- vend_dev_itens_completo %>%
      mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
    gravar_tabela("bruto", "tb_vend_dev_itens", vend_dev_itens_completo)

  }

  # if (nrow(revendas_completo)>0){
  #
  #   # cria a tabela com os dados do dataframe
  #   revendas_completo <- revendas_completo %>%
  #     mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
  #   gravar_tabela("bruto", "tb_revendas", revendas_completo)
  # }
  #
  if (nrow(preco_completo)>0){

    # cria a tabela com os dados do dataframe
    preco_completo <- preco_completo %>%
      mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
    gravar_tabela("bruto", "tb_precos_prod", preco_completo)

  }

  if (nrow(pedidos_completo)>0){

    # cria a tabela com os dados do dataframe
    pedidos_completo <- unique(pedidos_completo) %>%
      mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
    gravar_tabela("bruto", "tb_pedidos", pedidos_completo)

  }

# })
