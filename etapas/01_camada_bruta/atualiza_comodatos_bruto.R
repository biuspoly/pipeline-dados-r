inicio = Sys.time()

library(tidyr)
library(dplyr)
library(glue)
library(parallel)
library(future)
library(future.apply)
library(lubridate)
library(stringi)
library(purrr)

# Configurações do banco --------------------------------------------------
# Versão pública: ERP e MySQL substituídos por Parquet (ver R/funcoes_pipeline.R)

plan(sequential)

empresas = data.frame()

empresas <- rbind(empresas, data.frame(cnpj="11222333000101", Nome="Alfa", banco="ERP_ALFA", digito=1, tabela_prod=1, prefixo = ""))
empresas <- rbind(empresas, data.frame(cnpj="11222444000102", Nome="Beta Leste", banco="ERP_BETA_LESTE", digito=1, tabela_prod=1, prefixo = ""))
empresas <- rbind(empresas, data.frame(cnpj="11222444000203", Nome="Beta Oeste", banco="ERP_BETA_OESTE", digito=2, tabela_prod=1, prefixo = ""))
empresas <- rbind(empresas, data.frame(cnpj="11222555000104", Nome="Gama Norte", banco="ERP_GAMA_NORTE", digito=1, tabela_prod=1, prefixo = ""))
empresas <- rbind(empresas, data.frame(cnpj="11222666000105", Nome="Delta", banco="ERP_DELTA", digito=1, tabela_prod=1, prefixo = ""))
empresas <- rbind(empresas, data.frame(cnpj="11222777000206", Nome="Epsilon", banco="ERP_EPSILON", digito=2, tabela_prod=1, prefixo = "ERP_EPSILON_AUX."))
empresas <- rbind(empresas, data.frame(cnpj="11222888000107", Nome="Gama Sul", banco="ERP_GAMA_SUL", digito=1, tabela_prod=1, prefixo = ""))
empresas <- rbind(empresas, data.frame(cnpj="11222888000208", Nome="Gama Centro", banco="ERP_GAMA_CENTRO", digito=2, tabela_prod=2, prefixo = ""))
empresas <- rbind(empresas, data.frame(cnpj="11222999000209", Nome="Zeta Polo 1", banco="ERP_ZETA_1", digito=2, tabela_prod=1, prefixo = ""))
empresas <- rbind(empresas, data.frame(cnpj="11222999000310", Nome="Zeta Polo 2", banco="ERP_ZETA_2", digito=3, tabela_prod=1, prefixo = ""))
empresas <- rbind(empresas, data.frame(cnpj="11222999000111", Nome="Zeta Polo 3", banco="ERP_ZETA_3", digito=1, tabela_prod=1, prefixo = ""))


# log_file_revenda = "Dados/revenda_execucao.log"
registros_comod = future_lapply(1:nrow(empresas), function(linha_empresa) {

  tryCatch({
    # for (linha_empresa in 1:nrow(empresas)){
    # linha_empresa = 1
    # con = dbConnect(odbc(), dsn = empresas$banco[linha_empresa], encoding = "UTF-8", charset = "utf8mb4")
    banco = empresas$banco[linha_empresa]
    print(glue("Conectou na empresa {empresas$Nome[linha_empresa]}"))

    #   cev_baixado = dbGetQuery(con, "
    # select * from ERP_COMODATO_BAIXADO_01 where CMB_CLIENTE = 10001")
    #
    #   cev_pendente = dbGetQuery(con, "
    # select * from ERP_COMODATO_PENDENTE_01 where CMP_CLIENTE = 10002 and CMP_PRODUTO between 600000 and 700000")

    titulos_abertos = consulta_erp(banco, "titulos_abertos", glue("
    SELECT
        TIT_CLIENTE AS CODCLI,
        TIT_VENDEDOR AS VEN,
        TIT_VENCIMENTO as TITULO_MAIS_ANTIGO
         from {empresas$prefixo[linha_empresa]}ERP_TITULOS_0{empresas$digito[linha_empresa]}
         WHERE TIT_MODALIDADE = 4
    "))

    cev = consulta_erp(banco, "cev", glue("
    SELECT
      CAST(CMP_CLIENTE as varchar(8)) as CODCLI,
      CMP_PRODUTO AS CODCOMP,
      CMP_CONTRATO as CEV,
      CMP_MOTIVO as MOT,
      CMP_DT_EMISSAO AS DTE,
      CMP_DT_VENC AS DTV,
      CMP_QTD as QTD,
      CMP_NF as NF,
      CMP_SERIE as SERIE,
      cast(CMP_PEDIDO as varchar(12)) AS NPED,
      PPD_PRE_PEDIDO AS PRE_PED,
      CMP_ROMANEIO AS ROMA,
      'PENDENTE' AS STATUS
      FROM {empresas$prefixo[linha_empresa]}ERP_COMODATO_PENDENTE_0{empresas$digito[linha_empresa]}
      LEFT JOIN ERP_PRE_PEDIDOS ON PPD_PEDIDO = CMP_PEDIDO
      where CAST(CAST(CMP_PEDIDO AS VARCHAR(6)) AS INT) >= 202401

    /*UNION ALL

    SELECT
      CAST(CMB_CLIENTE as varchar(8)) as CODCLI,
      CMB_PRODUTO AS CODCOMP,
      CMB_CONTRATO as CEV,
      CMB_MOTIVO as MOT,
      CMB_DT_EMISSAO AS DTE,
      CMB_DT_VENC AS DTV,
      CMB_QTD as QTD,
      CMB_NF as NF,
      CMB_SERIE as SERIE,
      cast(CMB_PEDIDO as varchar(12)) AS NPED,
      PPD_PRE_PEDIDO AS PRE_PED,
      CMB_ROMANEIO AS ROMA,
      'BAIXADO' AS STATUS
      FROM {empresas$prefixo[linha_empresa]}ERP_COMODATO_BAIXADO_0{empresas$digito[linha_empresa]}
      LEFT JOIN ERP_PRE_PEDIDOS ON PPD_PEDIDO = CMB_PEDIDO
      where CAST(CAST(CMB_PEDIDO AS VARCHAR(6)) AS INT) >= 202401
      */
    "))

    ### Nao vai pegar dados de domingo
    dia_ref = Sys.Date()-1

    if (as.POSIXlt(dia_ref)$wday == 0) {
      dia_ref <- Sys.Date() - 2
    }

    estoque = consulta_erp(banco, "estoque", glue("

    /* FORMULA ANTIGA
    SELECT
         KSD_PRODUTO AS CODCOMP,
         CASE WHEN KSD_SALDO_INI_UN + KSD_ENTRADAS_UN - KSD_SAIDAS_UN > 0 THEN 'SIM' ELSE 'NAO' END AS ESTOQUE
    FROM {empresas$prefixo[linha_empresa]}ERP_KARDEX_SALDO_{format(Sys.Date() - 1, '%y%m')}0{empresas$digito[linha_empresa]}
    WHERE KSD_DIA = {format(Sys.Date() - 1, '%d')} AND
         KSD_OCORRENCIA NOT IN (0, 999) AND
         KSD_SALDO_INI_UN + KSD_ENTRADAS_UN - KSD_SAIDAS_UN > 0*/

    SELECT
      KDI_PRODUTO AS CODCOMP,
      CASE WHEN ((KDI_SALDO_INICIAL + KDI_ENTRADAS) - KDI_SAIDAS) > 0 THEN 'SIM' ELSE 'NAO' END AS ESTOQUE
    FROM
      {empresas$prefixo[linha_empresa]}ERP_KARDEX_DIA_{format(dia_ref, '%y%m')}0{empresas$digito[linha_empresa]}
    WHERE KDI_DIA = {format(dia_ref, '%d')} AND
          ((KDI_SALDO_INICIAL + KDI_ENTRADAS) - KDI_SAIDAS) > 0
  "))

    livro_entrada_tudo = data.frame()
    livro_saida_tudo = data.frame()
    # dt_inicio = "2025-01-01"
    dt_inicio = Sys.Date()

    for(data_tabela in seq(as.Date(dt_inicio), Sys.Date(), by = "month")){

      # writeLines(glue("Conectou na empresa {empresas$Nome[linha_empresa]} com a data {as.Date(data_tabela)}"), con = log_file_revenda, sep = "\r\n", useBytes = TRUE)
      livro_entrada_cev = consulta_erp(banco, "livro_entrada_cev", glue("
      SELECT
        FIT_NF AS 'NF',
        CASE WHEN FEN_CFOP IN (1553,2553) THEN 'Dev Venda' else 'Dev Emprestimo' end as 'OPERACAO',
        FEN_DT_ENTRADA AS 'Data',
        FEN_CLIENTE as CLI,
        FIT_PRODUTO AS 'PRODUTO',
        RTRIM(P.PRD_DESCRICAO_GENERICA) AS PRODUTO_GENERICO,
        FIT_QTD AS 'QTD',
        FIT_QTD_UN AS 'QTDS',
        C.CLI_VENDEDOR AS VEN
        FROM {empresas$prefixo[linha_empresa]}ERP_FISCAL_ITENS_{format(as.Date(data_tabela), '%y')}0{empresas$digito[linha_empresa]}{format(as.Date(data_tabela), '%m')}  A
        INNER JOIN {empresas$prefixo[linha_empresa]}ERP_FISCAL_ENTRADAS_{format(as.Date(data_tabela), '%y')}0{empresas$digito[linha_empresa]}{format(as.Date(data_tabela), '%m')}
        ON FIT_NF = FEN_NF and CAST(FEN_CFOP AS VARCHAR(4)) = CAST(FIT_CFOP AS VARCHAR(3)) || CAST(FIT_CFOP_DIGITO AS VARCHAR(1))
        LEFT JOIN {empresas$prefixo[linha_empresa]}ERP_CLIENTES C ON CAST(C.CLI_REGIAO AS VARCHAR(4)) || RIGHT('000' || LTRIM(RTRIM(CAST(C.CLI_NUMERO AS VARCHAR(4)))),4) = CAST(FEN_CLIENTE AS VARCHAR(8))
        LEFT JOIN {empresas$prefixo[linha_empresa]}ERP_PRODUTOS_0{empresas$tabela_prod[linha_empresa]} P ON RIGHT('0000'||CAST(P.PRD_FAMILIA AS VARCHAR(3)),3) || RIGHT('0000'||CAST(P.PRD_NUMERO AS VARCHAR(3)),3) = CAST(FIT_PRODUTO AS VARCHAR(12))
        WHERE FEN_CFOP in (1553,2553,1909,2909)
        AND FIT_CNPJ_EMITENTE NOT IN
        ({paste0(as.numeric(empresas$cnpj[empresas$cnpj != empresas$cnpj[linha_empresa]]), collapse = \", \")})
      "))

      # 1908, 2908 - recebimento de emprestimo
      # 1909, 2909 - retorno de emprestimos enviados
      # 5908, 6908 - saida por emprestimo
      # 5909, 6909 - retorno de emprestimos recebidos

      livro_entrada_tudo = rbind(livro_entrada_tudo, livro_entrada_cev)

      if(empresas$Nome[linha_empresa] == "Beta Leste"){
        corrige_devemprestimo = "WHEN (FSA_CFOP = 5908 AND RTRIM(FSA_SERIE) = '2') THEN 'Dev Empréstimo'"
      }
      else{
        corrige_devemprestimo = ""
      }

      livro_saida_cev = consulta_erp(banco, "livro_saida_cev", glue("
      SELECT
        FIT_NF AS 'NF',
        CASE WHEN FSA_CFOP IN (5551,6551) THEN 'Vendi' {corrige_devemprestimo} else 'Emprestei' end as 'OPERACAO',
        cast(FSA_ANO AS VARCHAR(4)) || RIGHT('0' || CAST(FSA_MES AS VARCHAR(2)),2) || RIGHT('0' || CAST(FSA_DIA AS VARCHAR(2)),2) AS 'Data',
        FSA_CLIENTE as CLI,
        FIT_PRODUTO AS 'PRODUTO',
        RTRIM(P.PRD_DESCRICAO_GENERICA) AS PRODUTO_GENERICO,
        FIT_QTD AS 'QTD',
        FIT_QTD_UN AS 'QTDS',
        C.CLI_VENDEDOR AS VEN
        FROM {empresas$prefixo[linha_empresa]}ERP_FISCAL_ITENS_{format(as.Date(data_tabela), '%y')}0{empresas$digito[linha_empresa]}{format(as.Date(data_tabela), '%m')}
        INNER JOIN {empresas$prefixo[linha_empresa]}ERP_FISCAL_SAIDAS_{format(as.Date(data_tabela), '%y')}0{empresas$digito[linha_empresa]}{format(as.Date(data_tabela), '%m')} ON FIT_NF = FSA_NF and CAST(FSA_CFOP AS VARCHAR(4)) = CAST(FIT_CFOP AS VARCHAR(3)) || CAST(FIT_CFOP_DIGITO AS VARCHAR(1))
        LEFT JOIN {empresas$prefixo[linha_empresa]}ERP_CLIENTES C ON CAST(C.CLI_REGIAO AS VARCHAR(4)) || RIGHT('000' || LTRIM(RTRIM(CAST(C.CLI_NUMERO AS VARCHAR(4)))),4) = CAST(FSA_CLIENTE AS VARCHAR(8))
        LEFT JOIN {empresas$prefixo[linha_empresa]}ERP_PRODUTOS_0{empresas$tabela_prod[linha_empresa]} P ON RIGHT('0000'||CAST(P.PRD_FAMILIA AS VARCHAR(3)),3) || RIGHT('0000'||CAST(P.PRD_NUMERO AS VARCHAR(3)),3) = CAST(FIT_PRODUTO AS VARCHAR(12))
        WHERE FSA_CFOP IN (5551,6551,6908,5908)
        AND FIT_CNPJ_EMITENTE NOT IN
        ({paste0(as.numeric(empresas$cnpj[empresas$cnpj != empresas$cnpj[linha_empresa]]), collapse = \", \")})
      "))

      livro_saida_tudo = rbind(livro_saida_tudo, livro_saida_cev)
    }

    # on.exit(dbDisconnect(con))

    if (nrow(titulos_abertos) > 0){
      titulos_abertos$CNPJ_REV = empresas$cnpj[linha_empresa]
    }
    if (nrow(cev) > 0){
      cev$CNPJ_REV = empresas$cnpj[linha_empresa]
    }
    if (nrow(livro_entrada_tudo) > 0){
      livro_entrada_tudo$CNPJ_REV = empresas$cnpj[linha_empresa]
    }
    if (nrow(livro_saida_tudo) > 0){
      livro_saida_tudo$CNPJ_REV = empresas$cnpj[linha_empresa]
    }
    if (nrow(estoque) > 0){
      estoque$CNPJ_REV = empresas$cnpj[linha_empresa]
    }

    return(list(titulos_abertos, cev, livro_entrada_tudo, livro_saida_tudo, estoque))

  }, error = function(e){

    message(glue("Erro na empresa {empresas$Nome[linha_empresa]}: {e$message}"))

    return(NULL)
  })

}, future.seed = NULL)


titulos_completo = map_dfr(registros_comod, 1)
cev_completo = map_dfr(registros_comod, 2)
livro_entrada_tudo_completo = map_dfr(registros_comod, 3)
livro_saida_tudo_completo = map_dfr(registros_comod, 4)
estoque_completo = map_dfr(registros_comod, 5)


## Correcao, agora Polo 2 é junto com Polo 1
titulos_completo$CNPJ_REV[as.numeric(titulos_completo$VEN) > 400 &
                            as.numeric(titulos_completo$VEN) <= 499 &
                            titulos_completo$CNPJ_REV == "11222999000209"] <- "11222999000310"
titulos_completo = unique(titulos_completo)

livro_entrada_tudo_completo$CNPJ_REV[as.numeric(livro_entrada_tudo_completo$VEN) > 400 &
                                       as.numeric(livro_entrada_tudo_completo$VEN) <= 499 &
                                       livro_entrada_tudo_completo$CNPJ_REV == "11222999000209"] <- "11222999000310"

livro_saida_tudo_completo$CNPJ_REV[as.numeric(livro_saida_tudo_completo$VEN) > 400 &
                                     as.numeric(livro_saida_tudo_completo$VEN) <= 499 &
                                     livro_saida_tudo_completo$CNPJ_REV == "11222999000209"] <- "11222999000310"

# No servidor: dbConnect(RMariaDB::MariaDB(), ...) na base da camada bruta.
# Aqui cada tabela vira dados/bruto/<tabela>.parquet

if (nrow(titulos_completo)>0){

  # cria a tabela com os dados do dataframe
  clientes_completo <- titulos_completo %>%
    mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
  # apaga a tabela se já existir e grava
  gravar_tabela("bruto", "tb_titulos_abertos", titulos_completo)

}

if (nrow(cev_completo)>0){

  # cria a tabela com os dados do dataframe
  cev_completo <- cev_completo %>%
    mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
  gravar_tabela("bruto", "tb_cev", cev_completo)

}

if (nrow(livro_entrada_tudo_completo)>0){

  # cria a tabela com os dados do dataframe
  livro_entrada_tudo_completo <- livro_entrada_tudo_completo %>%
    mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
  # no servidor: delete from tb_livro_entrada_cev + dbAppendTable
  gravar_tabela("bruto", "tb_livro_entrada_cev", livro_entrada_tudo_completo)

}

if (nrow(livro_saida_tudo_completo)>0){

  # cria a tabela com os dados do dataframe
  livro_saida_tudo_completo <- livro_saida_tudo_completo %>%
    mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
  # no servidor: delete from tb_livro_saida_cev + dbAppendTable
  gravar_tabela("bruto", "tb_livro_saida_cev", livro_saida_tudo_completo)

}

if (nrow(estoque_completo)>0){

  # cria a tabela com os dados do dataframe
  estoque_completo <- estoque_completo %>%
    mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
  gravar_tabela("bruto", "tb_estoque_cev", estoque_completo)

}

final = Sys.time()

print(sprintf("Tempo total: %02dh %02dm %02ds",
              as.integer(difftime(final, inicio,units = "secs")/3600),
              as.integer(difftime(final, inicio,units = "secs")/60),
              as.integer(difftime(final, inicio,units = "secs")))
)
