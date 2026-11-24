library(tidyr)
library(dplyr)
library(glue)
library(parallel)
library(future)
library(future.apply)

# capture.output(type = "message", file = "Dados/atualiza_pde_log.txt", {
  # Configurações do banco --------------------------------------------------
  # Versão pública: ERP e MySQL substituídos por Parquet (ver R/funcoes_pipeline.R)


  empresas = data.frame()

  empresas <- rbind(empresas, data.frame(cnpj="11222333000101", Nome="Alfa", banco="ERP_ALFA", digito=1, prefixo = "", transferencia = ""))
  empresas <- rbind(empresas, data.frame(cnpj="11222444000102", Nome="Beta Leste", banco="ERP_BETA_LESTE", digito=1, prefixo = "", transferencia = I(list(c(900001)))))
  empresas <- rbind(empresas, data.frame(cnpj="11222444000203", Nome="Beta Oeste", banco="ERP_BETA_OESTE", digito=2, prefixo = "", transferencia = I(list(c(900002)))))
  empresas <- rbind(empresas, data.frame(cnpj="11222555000104", Nome="Gama Norte", banco="ERP_GAMA_NORTE", digito=1, prefixo = "", transferencia = ""))
  empresas <- rbind(empresas, data.frame(cnpj="11222666000105", Nome="Delta", banco="ERP_DELTA", digito=1, prefixo = "", transferencia = ""))
  empresas <- rbind(empresas, data.frame(cnpj="11222777000206", Nome="Epsilon", banco="ERP_EPSILON", digito=2, prefixo = "ERP_EPSILON_AUX.", transferencia = ""))
  empresas <- rbind(empresas, data.frame(cnpj="11222888000107", Nome="Gama Sul", banco="ERP_GAMA_SUL", digito=1, prefixo = "", transferencia = ""))
  empresas <- rbind(empresas, data.frame(cnpj="11222888000208", Nome="Gama Centro", banco="ERP_GAMA_CENTRO", digito=2, prefixo = "", transferencia = ""))
  empresas <- rbind(empresas, data.frame(cnpj="11222999000209", Nome="Zeta Polo 1", banco="ERP_ZETA_1", digito=2, prefixo = "", transferencia = ""))
  empresas <- rbind(empresas, data.frame(cnpj="11222999000310", Nome="Zeta Polo 2", banco="ERP_ZETA_2", digito=3, prefixo = "", transferencia = ""))
  empresas <- rbind(empresas, data.frame(cnpj="11222999000111", Nome="Zeta Polo 3", banco="ERP_ZETA_3", digito=1, prefixo = "", transferencia = ""))

  pde_completo = data.frame()

  plan(multisession, workers = min(4, detectCores()-1))

  registros_pde = future_lapply(1:nrow(empresas), function(linha_empresa) {
    # for (linha_empresa in 1:nrow(empresas)){

    # con = dbConnect(odbc(), dsn = empresas$banco[linha_empresa])
    banco = empresas$banco[linha_empresa]
    print(glue("Conectou na empresa {empresas$Nome[linha_empresa]}"))

    romaneio = consulta_erp(banco, "romaneio_pde", glue("
    SELECT
      c.ROMANEIO,
      c.PLACA,
      c.MOTORISTA,
      c.ROTA,
      c.CAPACIDADE,
      c.PESO_ROMANEIO,
      c.QTD_PEDIDOS,
      p.QTD_ENT,
      c.DATA,
      p.FAT_SAIDA,
      p.QTD_DEV,
      p.FAT_DEV,
      c.HORA_ENT,
      c.MINUTO_ENT,
      c.KM_SAIDA,
      c.KM_CHEGADA,
      p.CLIENTE
    FROM (
      SELECT
        CAST(ROM_ANO AS VARCHAR(4)) ||
        RIGHT('00' || LTRIM(RTRIM(CAST(ROM_MES AS VARCHAR(2)))), 2) ||
        RIGHT('00000' || LTRIM(RTRIM(CAST(ROM_SEQ AS VARCHAR(5)))), 5) AS ROMANEIO,
        ROM_PLACA AS PLACA,
        ROM_DT_BAIXA AS DATA,
        ROM_PESO AS PESO_ROMANEIO,
        VEI_CAPACIDADE AS CAPACIDADE,
        ROM_MOTORISTA AS MOTORISTA,
        ROM_HORA_ENTRADA AS HORA_ENT,
        ROM_MIN_ENTRADA AS MINUTO_ENT,
        ROM_KM_INICIAL AS KM_SAIDA,
        ROM_KM_FINAL AS KM_CHEGADA,
        ROM_ROTA AS ROTA,
        ROM_QTD_PEDIDOS AS QTD_PEDIDOS
      FROM {empresas$prefixo[linha_empresa]}ERP_ROMANEIOS_0{empresas$digito[linha_empresa]}
      LEFT JOIN {empresas$prefixo[linha_empresa]}ERP_VEICULOS_0{empresas$digito[linha_empresa]} ON ROM_PLACA = VEI_PLACA
    ) c
    LEFT JOIN (
      SELECT
        CAST(PED_ROMANEIO AS VARCHAR(12)) AS ROMANEIO,
        PED_CLIENTE AS CLIENTE,
        COUNT(DISTINCT PED_CLIENTE) AS QTD_ENT,
        SUM(PED_VALOR) + SUM(PED_VALOR_OUTROS) AS FAT_SAIDA,
        SUM(CASE WHEN PED_DT_NF_DEVOLUCAO <> 0 THEN 1 ELSE 0 END) AS QTD_DEV,
        SUM(CASE WHEN PED_DT_NF_DEVOLUCAO <> 0 THEN PED_VALOR ELSE 0 END) +
        SUM(CASE WHEN PED_DT_NF_DEVOLUCAO <> 0 THEN PED_VALOR_OUTROS ELSE 0 END) AS FAT_DEV

      FROM {empresas$prefixo[linha_empresa]}ERP_PEDIDOS_0{empresas$digito[linha_empresa]}
      GROUP BY CAST(PED_ROMANEIO AS VARCHAR(12)), PED_CLIENTE
    ) p ON c.ROMANEIO = p.ROMANEIO
    WHERE c.ROMANEIO >= '{format(Sys.Date()-1,'%Y%m%d')}000'
  "))
    # WHERE c.ROMANEIO >= '{format(Sys.Date()-1,'%Y%m%d')}000'
    # versão pública: o filtro do WHERE é aplicado aqui sobre o Parquet de origem
    romaneio = romaneio[romaneio$ROMANEIO >= glue("{format(Sys.Date()-1,'%Y%m%d')}000"), ]
    # dbDisconnect(con)

    if (nrow(romaneio) > 0){

      romaneio = romaneio %>%
        filter(!(CLIENTE %in% unlist(empresas$transferencia[linha_empresa]))) %>%
        group_by(across(-c(FAT_SAIDA, QTD_ENT, FAT_DEV, QTD_DEV, CLIENTE))) %>%
        summarise(
          FAT_SAIDA = sum(FAT_SAIDA, na.rm = TRUE),
          QTD_ENT   = sum(QTD_ENT, na.rm = TRUE),
          FAT_DEV = sum(FAT_DEV, na.rm = TRUE),
          QTD_DEV   = sum(QTD_DEV, na.rm = TRUE),
          .groups = "drop"
        ) %>%
        relocate(FAT_SAIDA, .after = DATA) %>%
        relocate(QTD_ENT, .after = QTD_PEDIDOS)%>%
        relocate(QTD_DEV, .after = FAT_SAIDA)%>%
        relocate(FAT_DEV, .after = QTD_DEV)

      romaneio$DT_SAIDA = as.integer(format(as.Date(substr(romaneio$ROMANEIO, 1, 8),format = "%Y%m%d") + 1, "%Y%m%d"))
      romaneio$DIAS_ROTA = rep(1)
      romaneio$AJUDANTE_1 = rep(NA)
      romaneio$AJUDANTE_2 = rep(NA)
      romaneio$OBS = rep(NA)
      romaneio$HORA_ENT <- sprintf("%02d", as.integer(romaneio$HORA_ENT))
      romaneio$MINUTO_ENT <- sprintf("%02d", as.integer(romaneio$MINUTO_ENT))
      romaneio = unite(romaneio, col = "HR_FIM", c("HORA_ENT", "MINUTO_ENT"), sep = ":", remove = TRUE)

      pde = romaneio %>%
        group_by(DT_SAIDA) %>%
        arrange(ROMANEIO) %>%
        mutate(ID = seq_along(ROMANEIO))

      pde$CNPJ_EMP = rep(empresas$cnpj[linha_empresa])

      return(pde)
    }

  }, future.seed = NULL)

  plan(sequential)

  pde_completo = bind_rows(registros_pde)

  if (nrow(pde_completo)>0){
    pde_completo$ROTA <- stringi::stri_trans_general(pde_completo$ROTA, "Latin-ASCII")
    pde_completo$ROTA <- gsub("[^a-zA-Z0-9\\/\\(\\) \\-]", "", pde_completo$ROTA)

    # Renomear as colunas
    names(pde_completo) <- c("ROMANEIO", "PLACA", "MOTORISTA", "ROTA", "PESO_V", "PESO_R",
                             "QTD_NF", "QTD_ENT", "DT_FIM", "FAT_SAIDA", "QTD_DEV",
                             "FAT_DEV", "HR_FIM", "KM_INI", "KM_FIM", "DT_SAIDA",
                             "DIAS_ROTA", "AJUDANTE_1", "AJUDANTE_2", "OBS", "ID", "CNPJ_EMP")
    pde_completo$HR_SAIDA = rep(NA)
    # Reordene as colunas do data.frame para coincidir com a ordem do banco
    colunas_ordem <- c(
      "ID", "CNPJ_EMP", "ROMANEIO", "DT_SAIDA", "HR_SAIDA", "DIAS_ROTA", "PLACA", "MOTORISTA",
      "AJUDANTE_1", "AJUDANTE_2", "ROTA", "OBS", "PESO_V", "PESO_R", "QTD_ENT", "QTD_NF",
      "FAT_SAIDA", "QTD_DEV", "FAT_DEV", "DT_FIM", "HR_FIM", "KM_INI", "KM_FIM"
    )

    # Verifica se todas as colunas estão presentes
    missing_cols <- setdiff(colunas_ordem, names(pde_completo))
    if (length(missing_cols) > 0) {
      stop("As colunas seguintes estão faltando no data.frame: ", paste(missing_cols, collapse = ", "))
    }

    pde_completo <- pde_completo[, colunas_ordem]

    # No servidor: um INSERT INTO tb_pde por linha (linhas já existentes eram
    # recusadas pela chave da tabela e só geravam mensagem de erro no log).
    # Versão pública: acrescenta no Parquet mantendo a primeira ocorrência de cada romaneio.
    pde_completo = as.data.frame(pde_completo)
    pde_completo[c("HR_SAIDA", "AJUDANTE_1", "AJUDANTE_2", "OBS")] = lapply(pde_completo[c("HR_SAIDA", "AJUDANTE_1", "AJUDANTE_2", "OBS")], as.character)
    anexar_tabela("apps/pde", "tb_pde", pde_completo)
    tb_pde = ler_tabela("apps/pde", "tb_pde")
    gravar_tabela("apps/pde", "tb_pde", tb_pde[!duplicated(tb_pde[, c("CNPJ_EMP", "ROMANEIO")]), ])
    message(sprintf("%d linhas de PDE processadas.", nrow(pde_completo)))
  }
# })
