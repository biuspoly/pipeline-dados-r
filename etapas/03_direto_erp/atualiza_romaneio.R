library(tidyr)
library(dplyr)
library(glue)
library(parallel)
library(future)
library(future.apply)
library(lubridate)

# Último romaneio de cada placa (app de Frotas) ---------------------------

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

ult_rom_completo = data.frame()

plan(multisession, workers = min(4, detectCores()-1))

registros_ult_rom = future_lapply(1:nrow(empresas), function(linha_empresa) {
  # for (linha_empresa in 1:nrow(empresas)){

  # con = dbConnect(odbc(), dsn = empresas$banco[linha_empresa])
  banco = empresas$banco[linha_empresa]
  print(glue("Conectou na empresa {empresas$Nome[linha_empresa]}"))

  romaneio = consulta_erp(banco, "ultimo_romaneio", glue("
  SELECT
    max(CASE
        WHEN LENGTH(CAST(c.ROM_MES AS VARCHAR(30))) = 1 THEN
            CASE
                WHEN LENGTH(CAST(c.ROM_SEQ AS VARCHAR(30))) = 4 THEN
                    CAST(c.ROM_ANO AS VARCHAR(30)) || '0' || CAST(c.ROM_MES AS VARCHAR(30)) || '0' || CAST(c.ROM_SEQ AS VARCHAR(30))
                ELSE
                    CAST(c.ROM_ANO AS VARCHAR(30)) || '0' || CAST(c.ROM_MES AS VARCHAR(30)) || CAST(c.ROM_SEQ AS VARCHAR(30))
            END
        ELSE
            CASE
                WHEN LENGTH(CAST(c.ROM_SEQ AS VARCHAR(30))) = 4 THEN
                    CAST(c.ROM_ANO AS VARCHAR(30)) || CAST(c.ROM_MES AS VARCHAR(30)) || '0' || CAST(c.ROM_SEQ AS VARCHAR(30))
                ELSE
                    CAST(c.ROM_ANO AS VARCHAR(30)) || CAST(c.ROM_MES AS VARCHAR(30)) || CAST(c.ROM_SEQ AS VARCHAR(30))
            END
    END) AS ULT_ROMANEIO,
    UPPER(c.ROM_PLACA) as PLACA
  FROM {empresas$prefixo[linha_empresa]}ERP_ROMANEIOS_0{empresas$digito[linha_empresa]} c
  WHERE
  CASE
          WHEN LENGTH(CAST(c.ROM_MES AS VARCHAR(30))) = 1 THEN
              CASE
                  WHEN LENGTH(CAST(c.ROM_SEQ AS VARCHAR(30))) = 4 THEN
                      CAST(c.ROM_ANO AS VARCHAR(30)) || '0' || CAST(c.ROM_MES AS VARCHAR(30)) || '0' || CAST(c.ROM_SEQ AS VARCHAR(30))
                  ELSE
                      CAST(c.ROM_ANO AS VARCHAR(30)) || '0' || CAST(c.ROM_MES AS VARCHAR(30)) || CAST(c.ROM_SEQ AS VARCHAR(30))
              END
          ELSE
              CASE
                  WHEN LENGTH(CAST(c.ROM_SEQ AS VARCHAR(30))) = 4 THEN
                      CAST(c.ROM_ANO AS VARCHAR(30)) || CAST(c.ROM_MES AS VARCHAR(30)) || '0' || CAST(c.ROM_SEQ AS VARCHAR(30))
                  ELSE
                      CAST(c.ROM_ANO AS VARCHAR(30)) || CAST(c.ROM_MES AS VARCHAR(30)) || CAST(c.ROM_SEQ AS VARCHAR(30))
              END
      END >= '{format(lubridate::make_date(lubridate::year(Sys.Date()) - 1, 1, 1), '%Y%m')}00000'
  group by UPPER(c.ROM_PLACA)
  "))
  # {format(lubridate::`%m-%`(Sys.Date(), months(3)),'%Y%m')}00000 # pegar de tres meses atras
  # WHERE c.ROMANEIO >= '{format(Sys.Date()-1,'%Y%m%d')}000'
  # dbDisconnect(con)

  if (nrow(romaneio) > 0){

    romaneio$CNPJ_EMP = empresas$cnpj[linha_empresa]
    romaneio$ULT_ROMANEIO = substr(romaneio$ULT_ROMANEIO,1,8)

    return(romaneio)
  }

}, future.seed = NULL)

plan(sequential)

ult_rom_completo = bind_rows(registros_ult_rom)

if (nrow(ult_rom_completo)>0){

  ult_rom_completo = ult_rom_completo %>%
    # filter(trimws(PLACA) == "FIC-001") %>%
    group_by(PLACA) %>%
    summarise(ULT_ROMANEIO = max(ULT_ROMANEIO), CNPJ_EMP = CNPJ_EMP[which.max(ULT_ROMANEIO)])

  # No servidor: delete from tb_ult_rom + um INSERT por linha na base de Frotas.
  # Versão pública: regrava dados/apps/frotas/tb_ult_rom.parquet
  gravar_tabela("apps/frotas", "tb_ult_rom", ult_rom_completo[, c("CNPJ_EMP", "PLACA", "ULT_ROMANEIO")])
  message(sprintf("%d placas atualizadas.", nrow(ult_rom_completo)))
}
