library(stringr)
library(dplyr)
library(tidyr)
library(lubridate)
library(glue)



# Configurações do banco --------------------------------------------------
# Versão pública: MySQL substituído por Parquet (ver R/funcoes_pipeline.R)



# Carregar dados ----------------------------------------------------------

filtro_data = format(Sys.Date() %m-% months(2),'%Y%m')

# SELECT * FROM TB_PEDIDOS WHERE DT >= 20260100 AND DT >= {filtro_data}00
pedidos = ler_tabela("bruto", "tb_pedidos") %>% filter(DT >= 20260100, DT >= as.numeric(glue("{filtro_data}00")))
clientes = ler_tabela("bruto", "tb_clientes") #Clientes de cpf
# SELECT * FROM TB_VEND_DEV_ITENS WHERE COD_PED >= {filtro_data}000000
vend_dev_prod = ler_tabela("bruto", "tb_vend_dev_itens") %>% filter(COD_PED >= as.numeric(glue("{filtro_data}000000")))


# Tratamento de dados -----------------------------------------------------

clientes$CHAVE_CLI = paste0(clientes$CODCLI, clientes$CNPJ_REV)

pedidos$CHAVE_CLI = paste0(pedidos$CODCLI, pedidos$CNPJ_REV)

pedidos = left_join(pedidos, clientes[,c("FANTASIA","CHAVE_CLI", "CGC_CPF", "COD_CATEGORIA", "CANAL")],by = "CHAVE_CLI")

bonif_sku_marca_x = vend_dev_prod %>%
  # filter(COD_PED == "202611140001") %>%
  group_by(COD_PED, CNPJ_REV) %>%
  mutate(TEM_MARCA_A330_bonif = sum(CODR == "800001" & CFOP %in% c("5910","1910")), ## CFOP de bonificacao
         TEM_MARCA_X_vend = sum(CODR == "800002" & !(CFOP %in% c("5910","1910")))
  ) %>%
  ungroup()


vend_dev_prod = vend_dev_prod %>%
  group_by(COD_PED, CNPJ_REV) %>%
  summarise(QTD_SKUS = n_distinct(CODR))


pedidos = left_join(pedidos, vend_dev_prod, by = c("PEDIDO" = "COD_PED", "CNPJ_REV"))
pedidos = left_join(pedidos, unique(bonif_sku_marca_x[,c("COD_PED", "CNPJ_REV", "TEM_MARCA_A330_bonif", "TEM_MARCA_X_vend")]), by = c("PEDIDO" = "COD_PED", "CNPJ_REV"))

pedidos$STATUS[pedidos$STATUS %in% c(2,7)] = "Bloqueado"
pedidos$STATUS[pedidos$STATUS %in% c(9)] = "Cancelado"
pedidos$MES = substr(pedidos$DT, 1, 6)
pedidos = unique(pedidos)

pedidos_valor_mensal = pedidos %>%
  filter(!is.na(FANTASIA),
         !(STATUS %in% c("Bloqueado", "Cancelado")),
         CGC_CPF == "0000",
         OCO %in% c("1", "60")) %>%
  group_by(CHAVE_CLI) %>%
  filter(DT < max(DT)) %>%
  group_by(CODCLI, FANTASIA, MES, CHAVE_CLI) %>%
  summarise(FAT_MENSAL = sum(VALOR))

pedidos_valor_diario = pedidos %>%
  filter(!is.na(FANTASIA),
         !(STATUS %in% c("Bloqueado", "Cancelado")),
         CGC_CPF == "0000",
         OCO != "50") %>%
  group_by(CODCLI, FANTASIA, DT, CHAVE_CLI) %>%
  summarise(FAT_DIARIO = sum(VALOR))


pedidos_canc = pedidos %>%
  filter(STATUS %in% c("Bloqueado", "Cancelado"),
         !is.na(FANTASIA),
         CGC_CPF == "0000")

pedidos_canc_sku <- pedidos %>%
  mutate(CANAL = trimws(CANAL)) %>%
  filter(
    STATUS %in% c("Bloqueado", "Cancelado"),
    OCO %in% c("1", "7"),
    (COD_CATEGORIA %in% c("B3", "B4", "D8", "E7", "E8", "E9", "J1", "J2", "J3") & QTD_SKUS < 2) |
      (COD_CATEGORIA %in% c("K1", "L1", "L2", "L3", "L4", "L5", "L6", "L7") & QTD_SKUS < 3) |
      (!(COD_CATEGORIA %in% c("B3", "B4", "D8", "E7", "E8", "E9", "J1", "J2", "J3", "K1", "L1", "L2", "L3", "L4", "L5", "L6", "L7")) & QTD_SKUS <= 1)
  )

pedidos_canc_marca_x = pedidos %>%
  filter(
    STATUS %in% c("Bloqueado", "Cancelado"),
    TEM_MARCA_A330_bonif >= 1,
    TEM_MARCA_X_vend == 0)

# Juntar tabela de pedidos ------------------------------------------------

pedidos_canc$CHAVE_FAT_DIARIO = paste0(pedidos_canc$CHAVE_CLI, pedidos_canc$DT)
pedidos_canc$CHAVE_FAT_MENSAL = paste0(pedidos_canc$CHAVE_CLI, pedidos_canc$MES)

pedidos_valor_diario$CHAVE_FAT_DIARIO = paste0(pedidos_valor_diario$CHAVE_CLI, pedidos_valor_diario$DT)
pedidos_valor_mensal$CHAVE_FAT_MENSAL = paste0(pedidos_valor_mensal$CHAVE_CLI, pedidos_valor_mensal$MES)

historico_dlp = left_join(pedidos_canc[,c("CODCLI","FANTASIA","PEDIDO","DT","CHAVE_CLI","VALOR","CHAVE_FAT_MENSAL","CHAVE_FAT_DIARIO", "CNPJ_REV")],
                          pedidos_valor_diario[,c("CHAVE_FAT_DIARIO","FAT_DIARIO")], by = "CHAVE_FAT_DIARIO")
historico_dlp = left_join(historico_dlp, pedidos_valor_mensal[,c("CHAVE_FAT_MENSAL","FAT_MENSAL")], by = "CHAVE_FAT_MENSAL")


# Tratar tabela final -----------------------------------------------------

historico_dlp = unique(historico_dlp[,c("CNPJ_REV","CODCLI","FANTASIA","PEDIDO","DT","VALOR","FAT_DIARIO","FAT_MENSAL")])
historico_dlp$LIMITE_CPF = ifelse(historico_dlp$CNPJ_REV == "11222333000101", 12000, 30000)
historico_dlp$ATINGE_LIMITE = ifelse(historico_dlp$FAT_DIARIO + historico_dlp$FAT_MENSAL > historico_dlp$LIMITE_CPF, "SIM","NAO")
historico_dlp = historico_dlp %>%
  filter(ATINGE_LIMITE == "SIM")

historico_dlp_sku = unique(pedidos_canc_sku[,c("CNPJ_REV","CODCLI","FANTASIA","CANAL","PEDIDO","DT","VALOR","QTD_SKUS")])

historico_dlp_marca_x = unique(pedidos_canc_marca_x[,c("CNPJ_REV","CODCLI","FANTASIA","PEDIDO","DT","VALOR")])
# Subir dados -------------------------------------------------------------
# No servidor era a base do app de Pedidos cancelados; aqui dados/apps/pedidos_cancelados/


if (nrow(historico_dlp)>0){

  # apaga a tabela se já existir e cria com os dados do dataframe
  gravar_tabela("apps/pedidos_cancelados", "tb_historico_dlp", historico_dlp)

}

if (nrow(historico_dlp_sku)>0){

  gravar_tabela("apps/pedidos_cancelados", "tb_historico_dlp_sku", historico_dlp_sku)

}

if (nrow(historico_dlp_marca_x)>0){

  gravar_tabela("apps/pedidos_cancelados", "tb_historico_dlp_marca_x", historico_dlp_marca_x)

}
