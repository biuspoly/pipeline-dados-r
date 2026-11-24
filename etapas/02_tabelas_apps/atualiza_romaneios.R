library(dplyr)

# capture.output(type = "message", file = "Dados/atualiza_romaneio_log.txt", {

# Configurações do banco --------------------------------------------------
# Versão pública: MySQL substituído por Parquet (ver R/funcoes_pipeline.R)

# Leitura da camada bruta (no servidor: select * from tb_... na base bruta)
clientes = ler_tabela("bruto", "tb_clientes")
motoristas = ler_tabela("bruto", "tb_motorista")
produtos = ler_tabela("bruto", "tb_produtos")
romaneios = ler_tabela("bruto", "tb_rom_dev")
pedidos = ler_tabela("bruto", "tb_pedidos")
vendedores = ler_tabela("bruto", "tb_vendedor")
vendas_devolucoes_itens = ler_tabela("bruto", "tb_vend_dev_itens")


clientes$CHAVE_CLI = paste0(clientes$CODCLI, clientes$CNPJ_REV)
clientes$CHAVE_VEN = paste0(clientes$VEN, clientes$CNPJ_REV)
motoristas$CHAVE_MOT = paste0(motoristas$COD, motoristas$CNPJ_REV)
produtos$CHAVE_PROD = paste0(produtos$CODRED, produtos$CNPJ_REV)
romaneios$CHAVE_ROM = paste0(romaneios$ROMANEIO, romaneios$CNPJ_REV)
romaneios$CNPJ_ROM = romaneios$CNPJ_REV
vendedores$CHAVE_VEN = paste0(vendedores$COD, vendedores$CNPJ_REV)
pedidos$CHAVE_PED = paste0(pedidos$PEDIDO, pedidos$CNPJ_REV)
pedidos$CHAVE_ROM = paste0(pedidos$ROMANEIO, pedidos$CNPJ_REV)
pedidos$CHAVE_CLI = paste0(pedidos$CODCLI, pedidos$CNPJ_REV)


names(motoristas)[names(motoristas) == "NOME"] = "NOME_MOT"
names(vendedores)[names(vendedores) == "NOME"] = "NOME_VEN"
names(vendedores)[names(vendedores) == "COD"] = "COD_VEN"
names(romaneios)[names(romaneios) == "DATA"] = "Romaneio aberto"

romaneios$`Romaneio aberto`[romaneios$`Romaneio aberto` != 0] = "Não"
romaneios$`Romaneio aberto`[romaneios$`Romaneio aberto` == 0] = "Sim"


romaneios$MES_ATUAL = ifelse(
  format(as.Date(substr(romaneios$ROMANEIO,1,8),format = "%Y%m%d"), "%b/%Y") == format(Sys.Date(), "%b/%Y"),
  "Mês Atual",
  format(as.Date(substr(romaneios$ROMANEIO,1,8),format = "%Y%m%d"), "%b/%Y"))

pedidos = left_join(pedidos, romaneios[,c("Romaneio aberto","CHAVE_ROM", "MOTORISTA","MES_ATUAL")], by = "CHAVE_ROM")
pedidos = left_join(pedidos, clientes[,c("NOME","CHAVE_CLI")], by = "CHAVE_CLI")

pedidos = pedidos %>% filter(`Romaneio aberto` == "Sim")
pedidos$As = ifelse(pedidos$COD_VENDEDOR == "138","Sim","Não")

romaneios_pendentes = pedidos %>%
  # filter(CNPJ_REV == "11222444000102") %>%
  group_by(ROMANEIO, MOTORISTA, As, CNPJ_REV) %>%
  summarise(VALOR = sum(VALOR,na.rm = TRUE), PEDIDOS = n_distinct(PEDIDO)) %>%
  mutate(Dias = as.integer(Sys.Date() - as.Date(substr(ROMANEIO,1,8),format = "%Y%m%d")),
         Data = as.Date(substr(ROMANEIO,1,8),format = "%Y%m%d")) %>%
  rename(Romaneio = "ROMANEIO", Valor = "VALOR", Pedidos = "PEDIDOS", Motorista = "MOTORISTA") %>%
  select(Data, Romaneio, Pedidos, Motorista, As, Dias, Valor, CNPJ_REV) %>%
  arrange(Data)


pedidos_pendentes_c_nfe = pedidos %>%
  # filter(CNPJ_REV == "11222444000102") %>%
  group_by(PEDIDO, ROMANEIO, CODCLI, NOME,COD_VENDEDOR,As,TIPO_COBRANCA,NOTA_FISCAL, CHAVE_NFE, CNPJ_REV) %>%
  summarise(VALOR = sum(VALOR,na.rm = TRUE))

pedidos_pendentes_c_mot = pedidos %>%
  group_by(PEDIDO, ROMANEIO, CODCLI, MOTORISTA, NOME,COD_VENDEDOR,As,TIPO_COBRANCA,NOTA_FISCAL, CHAVE_NFE, CNPJ_REV) %>%
  summarise(VALOR = sum(VALOR,na.rm = TRUE))

vendas_devolucoes_itens_FILTRADO = vendas_devolucoes_itens # %>% filter(CNPJ_REV == "11222444000102")
vendas_devolucoes_itens_FILTRADO$CHAVE_PED = paste0(vendas_devolucoes_itens_FILTRADO$COD_PED, vendas_devolucoes_itens_FILTRADO$CNPJ_REV)

pedidos_pendentes_c_mot$VALOR = NULL
pedidos_pendentes_c_mot$CHAVE_PED = paste0(pedidos_pendentes_c_mot$PEDIDO, pedidos_pendentes_c_mot$CNPJ_REV)

produtos_pendentes = left_join(pedidos_pendentes_c_mot, vendas_devolucoes_itens_FILTRADO[,c("CODR","PRD_DESCRICAO_GENERICA","QTD","VALOR VENDA","CFOP", "CHAVE_PED")],by = c("CHAVE_PED" = "CHAVE_PED"),relationship = "many-to-many") %>%
  rename(VALOR = `VALOR VENDA`)
produtos_pendentes$CHAVE_PED = NULL

# length(unique(pedidos_pendentes$PEDIDO))
# length(unique(romaneios_pendentes$Romaneio))


# Criacao de tabelas ------------------------------------------------------
# No servidor era a base do app de Romaneios pendentes; aqui dados/apps/romaneios_pendentes/


if (nrow(romaneios_pendentes)>0){

  # cria a tabela com os dados do dataframe
  romaneios_pendentes <- romaneios_pendentes %>%
    mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
  # apaga a tabela se já existir e grava
  gravar_tabela("apps/romaneios_pendentes", "tb_rom_pen", romaneios_pendentes)

}

if (nrow(pedidos_pendentes_c_nfe)>0){

  # cria a tabela com os dados do dataframe
  pedidos_pendentes_c_nfe <- pedidos_pendentes_c_nfe %>%
    mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
  gravar_tabela("apps/romaneios_pendentes", "tb_ped_pen", pedidos_pendentes_c_nfe)

}

if (nrow(produtos_pendentes)>0){

  # cria a tabela com os dados do dataframe
  produtos_pendentes <- produtos_pendentes %>%
    mutate(across(where(is.character), ~ iconv(., from = "latin1", to = "UTF-8", sub = "")))
  gravar_tabela("apps/romaneios_pendentes", "tb_prod_pen", produtos_pendentes)

}

# })
