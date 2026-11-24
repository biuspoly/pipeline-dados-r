library(dplyr)
library(lubridate)
library(glue)

# Configurações do banco --------------------------------------------------
# Versão pública: MySQL substituído por Parquet (ver R/funcoes_pipeline.R)





# Funcao de mês e data ----------------------------------------------------

data_calend = function(){
  seq_meses <- rev(seq(from = as.Date("2025-01-01"), to = Sys.Date(), by = "month"))

  meses_dev <- format(seq_meses, "%b/%Y")
  meses_dev[meses_dev == format(Sys.Date(), "%b/%Y")] <- "Mês Atual"

  # Inicializar listas para armazenar colunas
  Meses <- c()
  Dias <- c()
  Labels <- c()

  # Preencher listas
  for(i in seq_along(seq_meses)) {
    mes <- seq_meses[i]
    dias_mes <- seq(from = mes,
                    to   = seq_meses[i] + months(1) - 1,
                    by   = "day")
    dias_mes <- rev(dias_mes[dias_mes <= Sys.Date()])

    Meses <- c(Meses, rep(meses_dev[i], length(dias_mes)))
    Dias  <- c(Dias, dias_mes)
    Labels <- c(Labels, format(dias_mes, "%d/%m/%Y"))
  }

  # Criar data.frame final
  df_datas <- data.frame(
    Mes = Meses,
    Dia = Dias,
    Label = Labels,
    stringsAsFactors = FALSE
  )

  df_datas
}



# Tratamento de dados -----------------------------------------------------



## Empresas para particionamento -------------------------------------------

empresas <- c(
  '11222333000101','11222444000102','11222444000203','11222555000104',
  '11222666000105','11222777000206','11222888000107','11222888000208',
  '11222999000209','11222999000310','11222999000111')

# --- Meses detectados dinamicamente
meses <- unique(data_calend()$Mes)

mes_map <- function(mes) {
  if(mes == "Mês Atual") return("atual")
  if(mes == head(meses, 2)[2]) return("ultimo")
  if(mes == head(meses, 3)[3]) return("penultimo")
  if(mes == head(meses, 4)[4]) return("antepenultimo")
  return("historico")
}

# Fazer tabelas -----------------------------------------------------------

# Leitura da camada bruta (no servidor: select * from tb_... na base bruta)
clientes = ler_tabela("bruto", "tb_clientes")
motoristas = ler_tabela("bruto", "tb_motorista")
produtos = ler_tabela("bruto", "tb_produtos")
romaneios = ler_tabela("bruto", "tb_rom_dev")
vendas_devolucoes = ler_tabela("bruto", "tb_vendas_e_dev")
vendedores = ler_tabela("bruto", "tb_vendedor")
vendas_devolucoes_itens = ler_tabela("bruto", "tb_vend_dev_itens")



clientes$CHAVE_CLI = paste0(clientes$CODCLI, clientes$CNPJ_REV)
clientes$CHAVE_VEN = paste0(clientes$VEN, clientes$CNPJ_REV)
motoristas$CHAVE_MOT = paste0(motoristas$COD, motoristas$CNPJ_REV)
produtos$CHAVE_PROD = paste0(produtos$CODRED, produtos$CNPJ_REV)
romaneios$CHAVE_ROM = paste0(romaneios$ROMANEIO, romaneios$CNPJ_REV)
vendedores$CHAVE_VEN = paste0(vendedores$COD, vendedores$CNPJ_REV)
vendas_devolucoes_itens$CHAVE_VDI = paste0(vendas_devolucoes_itens$COD_PED, vendas_devolucoes_itens$CNPJ_REV)
vendas_devolucoes_itens$URLImgCODR = paste0("https://exemplo.com/Produto/",vendas_devolucoes_itens$CODR,".png")
vendas_devolucoes$CHAVE_CLI = paste0(vendas_devolucoes$CLI, vendas_devolucoes$CNPJ_REV)
vendas_devolucoes$CHAVE_MOT = paste0(vendas_devolucoes$MOT, vendas_devolucoes$CNPJ_REV)
vendas_devolucoes$CHAVE_PROD = paste0(vendas_devolucoes$CODR, vendas_devolucoes$CNPJ_REV)
vendas_devolucoes$CHAVE_ROM = paste0(vendas_devolucoes$ROMANEIO, vendas_devolucoes$CNPJ_REV)
vendas_devolucoes$CHAVE_VEN = paste0(vendas_devolucoes$VEN, vendas_devolucoes$CNPJ_REV)
vendas_devolucoes$CHAVE_VDI = paste0(vendas_devolucoes$COD_PED, vendas_devolucoes$CNPJ_REV)

names(motoristas)[names(motoristas) == "NOME"] = "NOME_MOT"
names(vendedores)[names(vendedores) == "NOME"] = "NOME_VEN"
names(vendedores)[names(vendedores) == "COD"] = "COD_VEN"
names(romaneios)[names(romaneios) == "DATA"] = "Romaneio aberto"
names(vendas_devolucoes)[names(vendas_devolucoes) == "CMP_TIPO"] = "Comodato"

romaneios$`Romaneio aberto`[romaneios$`Romaneio aberto` != 0] = "Não"
romaneios$`Romaneio aberto`[romaneios$`Romaneio aberto` == 0] = "Sim"
vendas_devolucoes$Comodato[!is.na(vendas_devolucoes$Comodato)] = 1
vendas_devolucoes$Comodato[is.na(vendas_devolucoes$Comodato)] = 0
comodato_cliente = vendas_devolucoes %>% group_by(CHAVE_CLI) %>% summarise(`Comodato cliente` = ifelse(sum(as.numeric(Comodato)) > 0,"Sim","Não"))

clientes = left_join(clientes, unique(vendedores[,c("COD_VEN", "NOME_VEN", "CHAVE_VEN")]), by = "CHAVE_VEN")
vendas_devolucoes = left_join(vendas_devolucoes, clientes[,c("FANTASIA","CHAVE_CLI","CIDADE","NOME_VEN","COD_VEN")], by = "CHAVE_CLI")
vendas_devolucoes = left_join(vendas_devolucoes, produtos[,c("PRODUTO","CHAVE_PROD","PRODUTO_GENERICO")], by = "CHAVE_PROD")
vendas_devolucoes = left_join(vendas_devolucoes, motoristas[,c("NOME_MOT","CHAVE_MOT")], by = "CHAVE_MOT")
vendas_devolucoes = left_join(vendas_devolucoes, romaneios[,c("Romaneio aberto","CHAVE_ROM")], by = "CHAVE_ROM")
vendas_devolucoes = left_join(vendas_devolucoes, comodato_cliente[,c("CHAVE_CLI","Comodato cliente")], by = "CHAVE_CLI")
vendas_devolucoes = vendas_devolucoes %>% filter(Comodato == 0) %>% select(-c("CHAVE_CLI","CHAVE_ROM","CHAVE_MOT","CHAVE_VEN","CHAVE_PROD","VEN"))
vendas_devolucoes_itens = left_join(vendas_devolucoes_itens, unique(vendas_devolucoes[,c("CHAVE_VDI","TPCOBRA")]), by = "CHAVE_VDI")

vendas_devolucoes$MES_ATUAL = ifelse(
  format(as.Date(vendas_devolucoes$DATA_VENDA,format = "%Y%m%d"), "%b/%Y") == format(Sys.Date(), "%b/%Y"),
  "Mês Atual",
  format(as.Date(vendas_devolucoes$DATA_VENDA,format = "%Y%m%d"), "%b/%Y"))

vendas_devolucoes <- vendas_devolucoes %>%
  mutate(
    COD_VEN = case_when(
      CNPJ_REV == "11222999000209" & COD_VEN == "140" ~ "343",
      CNPJ_REV == "11222999000209" & COD_VEN == "141" ~ "344",
      CNPJ_REV == "11222999000209" & COD_VEN == "142" ~ "140",
      CNPJ_REV == "11222999000209" & COD_VEN == "143" ~ "141",
      CNPJ_REV == "11222999000209" & COD_VEN == "144" ~ "142",
      CNPJ_REV == "11222999000209" & COD_VEN == "340" ~ "738",
      CNPJ_REV == "11222999000209" & COD_VEN == "341" ~ "340",
      CNPJ_REV == "11222999000209" & COD_VEN == "342" ~ "341",
      CNPJ_REV == "11222999000209" & COD_VEN == "343" ~ "739",
      CNPJ_REV == "11222999000209" & COD_VEN == "344" ~ "342",
      CNPJ_REV == "11222999000209" & COD_VEN == "345" ~ "740",
      CNPJ_REV == "11222999000209" & COD_VEN == "346" ~ "741",
      TRUE ~ COD_VEN
    )
  )

vendas_devolucoes <- vendas_devolucoes %>%
  mutate(GRUPO_REV = case_when(
    substr(COD_VEN,1,1) == "2" & CNPJ_REV == "11222999000209" ~ "Zeta Polo 4",
    substr(COD_VEN,1,1) == "4" & CNPJ_REV == "11222999000209" ~ "Zeta Polo 2",
    substr(COD_VEN,1,1) == "2" & CNPJ_REV == "11222666000105" ~ "Delta Rota 2",
    substr(COD_VEN,1,1) == "3" & CNPJ_REV == "11222666000105" ~ "Delta Rota 3",
    substr(COD_VEN,1,1) == "1" & CNPJ_REV == "11222666000105" ~ "Delta Rota 1",
    substr(COD_VEN,1,1) == "1" & CNPJ_REV == "11222444000203" ~ "Beta Oeste",
    substr(COD_VEN,1,1) == "2" & CNPJ_REV == "11222444000203" ~ "Beta Oeste Rota 2",
    substr(COD_VEN,1,1) %in% c("4","6","9") & CNPJ_REV == "11222444000203" ~ "Beta Oeste Rota 3",
    substr(COD_VEN,1,1) == "3" & CNPJ_REV == "11222444000203" ~ "Beta Oeste Rota 4",
    substr(COD_VEN,1,3) %in% c("640") & CNPJ_REV == "11222444000102" ~ "Beta Leste Rota 2",
    substr(COD_VEN,1,1) %in% c("4") & CNPJ_REV == "11222444000102" ~ "Beta Leste Rota 2",
    substr(COD_VEN,1,1) %in% c("1", "6","7", "9") & CNPJ_REV == "11222444000102" ~ "Beta Leste",
    substr(COD_VEN,1,3) %in% c("738", "739") & CNPJ_REV == "11222666000105" ~ "Delta Rota 1",
    substr(COD_VEN,1,3) %in% c("740") & CNPJ_REV == "11222666000105" ~ "Delta Rota 2",
    substr(COD_VEN,1,3) %in% c("741") & CNPJ_REV == "11222666000105" ~ "Delta Rota 3",
    substr(COD_VEN, 1, 1) == "9" & CNPJ_REV == "11222333000101" ~ "Alfa Linha B",
    substr(COD_VEN,1,3) %in% c("742", "743") & CNPJ_REV == "11222999000209" ~ "Zeta Polo 2",
    substr(COD_VEN, 1, 3) %in% c("148","149","150","151","152","154","156","158","160","171","173","248","249","270","936") & CNPJ_REV == "11222333000101" ~ "Alfa Linha A",
    substr(COD_VEN, 1, 3) %in% c("153","155","159","161","162","163","164","170","172","174","250","252","253","269","271","272","273","348","349","350","738","739","740","741","742","743","744","745","836") & CNPJ_REV == "11222333000101" ~ "Alfa Linha B",
    TRUE ~ NA_character_   # caso não entre em nenhuma condição
  ))
vendas_devolucoes$CNPJ_REV = ifelse(substr(vendas_devolucoes$COD_VEN,1,1) == "2" & vendas_devolucoes$CNPJ_REV == "11222999000209",
                                    "11222999000310", vendas_devolucoes$CNPJ_REV)


## Calcula o retorno de pedidos por cliente em 3 meses

retorno_cli_rev <- vendas_devolucoes %>%
  mutate(MES_DATE = ymd(paste0(MES, "01"))) %>%  # transforma em data
  filter(
    MES_DATE >= floor_date(Sys.Date(), "month") %m-% months(2) &
      MES_DATE <= floor_date(Sys.Date(), "month")
  ) %>%
  group_by(CLI, CNPJ_REV) %>%
  summarise(
    vdi_total = n_distinct(CHAVE_VDI),
    vdi_dev   = n_distinct(CHAVE_VDI[`VALOR DEVOLUCAO` > 0]),
    taxa_media_dev = ifelse(vdi_total == 0, 0, vdi_dev / vdi_total),
    .groups = "drop"
  )

vendas_devolucoes = vendas_devolucoes %>%
  left_join(retorno_cli_rev, by = c("CLI", "CNPJ_REV"))

beta_leste = vendas_devolucoes %>%
  filter(GRUPO_REV == "Beta Leste",
         MES_ATUAL == "Mês Atual")

# Gravação: no servidor era a base do app de Devoluções; aqui dados/apps/devolucoes/


historico_removido <- setNames(rep(FALSE, length(empresas)), empresas)

for(cnpj in empresas) {
  # Subset para a empresa
  df_emp <- vendas_devolucoes %>% filter(CNPJ_REV == cnpj)

  # Iterar por cada "grupo de meses"
  for(mes in meses) {
    grupo <- mes_map(mes)

    df_sub <- df_emp %>% filter(MES_ATUAL == mes)

    if(nrow(df_sub) > 0) {
      # Criar nome da tabela genérica
      tablename <- glue("tb_vend_dev_{cnpj}_{grupo}")

      if(grupo == "historico") {
        if(!historico_removido[cnpj]) {
          # Primeira vez: remove e cria
          gravar_tabela("apps/devolucoes", tablename, df_sub)
          historico_removido[cnpj] <- TRUE
          print(glue("Criada tabela HISTÓRICO: {tablename} com {nrow(df_sub)} linhas (primeira vez)\n"))
        } else {
          # Demais vezes: apenas append
          anexar_tabela("apps/devolucoes", tablename, df_sub)
          print(glue("Atualizada tabela HISTÓRICO: {tablename} com {nrow(df_sub)} linhas (append)\n"))
        }
      } else {
        # Para outros grupos: remove e cria sempre
        gravar_tabela("apps/devolucoes", tablename, df_sub)
        print(glue("Criada tabela {grupo}: {tablename} com {nrow(df_sub)} linhas\n"))
      }
    }
  }
}

gravar_tabela("apps/devolucoes", "tb_vend_dev_itens", vendas_devolucoes_itens)
