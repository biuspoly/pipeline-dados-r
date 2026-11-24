library(tidyr)
library(dplyr)
library(glue)
library(lubridate)
library(stringr)
library(purrr)



# Configurações do banco --------------------------------------------------
# Versão pública: MySQL substituído por Parquet (ver R/funcoes_pipeline.R).
# As consultas SQL que rodavam na camada bruta foram reproduzidas em dplyr;
# o SQL original ficou comentado acima de cada uma.

# titulos_em_aberto     = dbGetQuery(con, "select CODCLI, CNPJ_REV, min(titulo_mais_antigo) as TITULO_MAIS_ANTIGO from tb_titulos_abertos group by CODCLI, CNPJ_REV")
titulos_em_aberto    = ler_tabela("bruto", "tb_titulos_abertos")
# select * from tb_cev WHERE nped >= 202400000000
cev                   = ler_tabela("bruto", "tb_cev") %>% filter(as.numeric(NPED) >= 202400000000)
# select *, case when dtv >= {hoje} then 'A VENCER' else 'VENCIDO' end as VENC from tb_cev
cev_vencidos_a_vencer = ler_tabela("bruto", "tb_cev") %>% mutate(VENC = ifelse(DTV >= as.numeric(format(Sys.Date(), '%Y%m%d')), "A VENCER", "VENCIDO"))
# select * from tb_clientes where ven <> '936'
clientes              = ler_tabela("bruto", "tb_clientes") %>% filter(VEN != "936")
produtos              = ler_tabela("bruto", "tb_produtos")
# pedidos               = ler_tabela("bruto", "tb_pedidos")

# select pr.*, CASE WHEN ec.ESTOQUE IS NULL THEN 'NAO' ELSE ec.ESTOQUE END AS ESTOQUE,
#        round(pp.PRECO_UN,2) AS PRECO, PP.TAB_CEV
#   from (select * from tb_produtos where cev = 'S') pr
#   left join tb_precos_prod pp on pp.codcomp = pr.codcomp and pp.cnpj_rev = pr.cnpj_rev
#   left join tb_estoque_cev ec on ec.codcomp = pr.codcomp and ec.cnpj_rev = pr.cnpj_rev
produtos_cev          = produtos %>%
  filter(CEV == "S") %>%
  left_join(ler_tabela("bruto", "tb_precos_prod") %>% select(CODCOMP, CNPJ_REV, PRECO_UN, TAB_CEV), by = c("CODCOMP", "CNPJ_REV")) %>%
  left_join(ler_tabela("bruto", "tb_estoque_cev") %>% mutate(CODCOMP = as.character(CODCOMP)), by = c("CODCOMP", "CNPJ_REV")) %>%
  mutate(ESTOQUE = ifelse(is.na(ESTOQUE), "NAO", ESTOQUE),
         PRECO = round(PRECO_UN, 2)) %>%
  select(-PRECO_UN) %>%
  relocate(TAB_CEV, .after = PRECO)

# select * from tb_livro_entrada_cev union all select * from tb_livro_saida_cev;
livro_fiscal          = rbind(ler_tabela("bruto", "tb_livro_entrada_cev"),
                              ler_tabela("bruto", "tb_livro_saida_cev"))
# anexos            = dbGetQuery(con, "SELECT DISTINCT CNPJ_REV,ncev,codcli,nf,tipo,situacao,usuario,anexo1,anexo2, anexo3,dataI from tb_anexos_cev where situacao <> 'X'")

# Cadastro de funcionários de um banco MySQL externo (no servidor: select * from funcionarios)
funcionarios_externo = consulta_mysql("externo", "funcionarios", "select * from funcionarios")


funcionarios_externo <- funcionarios_externo %>%
  mutate(CNPJ_REV = case_when(
    licenca == 1001 ~ "11222333000101",
    licenca == 1002 ~ "11222444000102",
    licenca == 1003 ~ "11222444000203",
    licenca == 1004 ~ "11222555000104",
    licenca == 1005 ~ "11222666000105",
    licenca == 1006 ~ "11222777000206",
    licenca == 1007 ~ "11222888000107",
    licenca == 1008 ~ "11222888000208",
    licenca == 1009 ~ "11222999000209",
    licenca == 1010 ~ "11222999000310",
    licenca == 1011 ~ "11222999000111",
    TRUE ~ NA_character_
  ))

produtos$UNIDADE = str_extract(produtos$PRODUTO, "(?i)\\d{1,3}(?:X\\d{1,2})?(?=\\s*U|UN)")

produtos$UNIDADE <- sapply(produtos$UNIDADE, function(x) {
  if (is.na(x)) return(NA)
  prod(as.numeric(unlist(strsplit(x, "X"))))
})
produtos$UNIDADE = ifelse(produtos$CODCOMP %in% c("700101", "700102"), 12, produtos$UNIDADE)
produtos$UNIDADE = ifelse(produtos$CODCOMP %in% c("700305", "700306"), 24, produtos$UNIDADE)

# titulos_em_aberto$Inadimplente = ifelse(Sys.Date() - as.Date(as.character(titulos_em_aberto$TITULO_MAIS_ANTIGO),"%Y%m%d") >= 30, "Sim", "Não")
titulos_em_aberto$Dia_Semana = as.numeric(format(as.Date(as.character(titulos_em_aberto$TITULO_MAIS_ANTIGO),"%Y%m%d"),format = "%u")) # 1 - segunda, 7 - domingo
# titulos_em_aberto$Inadimplente_1dia = ifelse(Sys.Date() - as.Date(as.character(titulos_em_aberto$TITULO_MAIS_ANTIGO),"%Y%m%d") >= 1 & !(titulos_em_aberto$Dia_Semana %in% c(6,7)), "Sim", "Não")
titulos_em_aberto$Inadimplente = ifelse(Sys.Date() - as.Date(as.character(titulos_em_aberto$TITULO_MAIS_ANTIGO),"%Y%m%d") >= 1, "Sim", "Não")
titulos_em_aberto$Inadimplente = ifelse(Sys.Date() - as.Date(as.character(titulos_em_aberto$TITULO_MAIS_ANTIGO),"%Y%m%d") == 1 & !(titulos_em_aberto$Dia_Semana %in% c(6,7)), "Sim", titulos_em_aberto$Inadimplente)

funcionarios_externo$CHAVE_VEN = paste0(funcionarios_externo$rca, funcionarios_externo$CNPJ_REV)
titulos_em_aberto$CHAVE_CLI = paste0(titulos_em_aberto$CODCLI,titulos_em_aberto$CNPJ_REV)
cev$CHAVE_CLI = paste0(cev$CODCLI, cev$CNPJ_REV)
cev_vencidos_a_vencer$CHAVE_CLI = paste0(cev_vencidos_a_vencer$CODCLI, cev_vencidos_a_vencer$CNPJ_REV)
clientes$CHAVE_CLI = paste0(clientes$CODCLI, clientes$CNPJ_REV)
clientes$CHAVE_VEN = paste0(clientes$VEN,clientes$CNPJ_REV)
cev$CHAVE_PROD = paste0(cev$CODCOMP, cev$CNPJ_REV)
cev_vencidos_a_vencer$CHAVE_PROD = paste0(cev_vencidos_a_vencer$CODCOMP, cev_vencidos_a_vencer$CNPJ_REV)
produtos$CHAVE_PROD = paste0(produtos$CODCOMP, produtos$CNPJ_REV)
# anexos$CHAVE_REVCEV = paste0(anexos$CNPJ_REV, anexos$ncev)
cev$CHAVE_REVCEV = paste0(cev$CNPJ_REV, cev$CEV)
cev_vencidos_a_vencer$CHAVE_REVCEV = paste0(cev_vencidos_a_vencer$CNPJ_REV, cev_vencidos_a_vencer$CEV)

clientes$FANTASIA = str_to_title(clientes$FANTASIA)
clientes$NOME = str_to_title(clientes$NOME)
produtos$PRODUTO_GENERICO = str_to_title(produtos$PRODUTO_GENERICO)

titulos_agrupados = titulos_em_aberto[,c("CHAVE_CLI", "Inadimplente", "TITULO_MAIS_ANTIGO", "Dia_Semana")] %>%
  filter(Inadimplente == "Sim",
         !(Dia_Semana %in% c(6,7)))%>%
  group_by(CHAVE_CLI, Inadimplente) %>%
  summarise(TITULO_MAIS_ANTIGO = max(TITULO_MAIS_ANTIGO))

cev = left_join(cev, titulos_agrupados[,c("CHAVE_CLI", "Inadimplente")], by = "CHAVE_CLI")
cev = left_join(cev, clientes[,c("CHAVE_CLI","FANTASIA", "NOME")], by = "CHAVE_CLI")
cev = left_join(cev, produtos[,c("CHAVE_PROD","PRODUTO_GENERICO", "TIPO")], by = "CHAVE_PROD")

cev_vencidos_a_vencer = left_join(cev_vencidos_a_vencer, titulos_agrupados[,c("CHAVE_CLI", "Inadimplente")], by = "CHAVE_CLI")
cev_vencidos_a_vencer = left_join(cev_vencidos_a_vencer, clientes[,c("CHAVE_CLI","FANTASIA", "NOME", "CANAL", "PORTE", "VEN")], by = "CHAVE_CLI")
cev_vencidos_a_vencer = left_join(cev_vencidos_a_vencer, produtos[,c("CHAVE_PROD","PRODUTO_GENERICO", "TIPO")], by = "CHAVE_PROD")



# cev = left_join(cev, anexos[,c("CHAVE_REVCEV","dataI", "situacao", "usuario","anexo1","anexo2","anexo3")], by = "CHAVE_REVCEV")

# pp = anexos %>%
#   group_by(CHAVE_REVCEV) %>%
#   summarise(n())

# cev$Intervalo = ifelse(
#   as.Date(as.character(cev$DTV), "%Y%m%d") - as.Date(as.character(cev$DTE), "%Y%m%d") == 0,
#   "Nenhum",
#   ifelse(
#     as.Date(as.character(cev$DTV), "%Y%m%d") - as.Date(as.character(cev$DTE), "%Y%m%d") == 1,
#     "1 dia",
#     paste(as.Date(as.character(cev$DTV), "%Y%m%d") - as.Date(as.character(cev$DTE), "%Y%m%d"), "dias",sep = " ")
#   )
# )

cev$Intervalo = as.integer(as.Date(as.character(cev$DTV), "%Y%m%d") - as.Date(as.character(cev$DTE), "%Y%m%d"))
cev$`Validacao Intervalo` = ifelse(as.Date(as.character(cev$DTV), "%Y%m%d") - as.Date(as.character(cev$DTE), "%Y%m%d") > 15 &
                                     as.Date(as.character(cev$DTV), "%Y%m%d") - as.Date(as.character(cev$DTE), "%Y%m%d") < 185,
                                   "Incorreto", NA)
cev$`Contrato vencido` = ifelse(Sys.Date() < cev$DTV, "Sim", NA)
cev_cliente = cev %>%
  group_by(CHAVE_CLI) %>%
  summarise("CEV_PEN" = n_distinct(CHAVE_REVCEV))

cev = cev %>% select(-c("CHAVE_CLI", "CHAVE_REVCEV", "CHAVE_PROD"))
cev$`Prazo Envio` = as.integer(10 - (Sys.Date() - as.Date(as.character(ifelse(cev$DTE < 20240507, 20250407, cev$DTE)),"%Y%m%d")))


cev$Inadimplente[is.na(cev$Inadimplente)] = "Não"


livro_fiscal <- livro_fiscal %>%
  mutate(GRUPO_REV = case_when(
    CNPJ_REV == "11222333000101" ~ "Alfa",
    CNPJ_REV == "11222444000102" ~ "Beta Leste",
    CNPJ_REV == "11222444000203" ~ "Beta Oeste",
    CNPJ_REV == "11222555000104" ~ "Gama Norte",
    CNPJ_REV == "11222666000105" ~ "Delta",
    CNPJ_REV == "11222777000206" ~ "Epsilon",
    CNPJ_REV == "11222888000107" ~ "Gama Sul",
    CNPJ_REV == "11222888000208" ~ "Gama Centro",
    CNPJ_REV == "11222999000209" ~ "Zeta Polo 1",
    CNPJ_REV == "11222999000310" ~ "Zeta Polo 2",
    CNPJ_REV == "11222999000111" ~ "Zeta Polo 3",
    TRUE ~ NA_character_  # default
  )) %>%
  mutate(GRUPO_REV = case_when(
    substr(VEN,1,1) == "2" & CNPJ_REV == "11222999000209" ~ "Zeta Polo 4",
    substr(VEN,1,1) == "4" & CNPJ_REV == "11222999000209" ~ "Zeta Polo 2",
    substr(VEN,1,1) == "2" & CNPJ_REV == "11222666000105" ~ "Delta",
    substr(VEN,1,1) == "3" & CNPJ_REV == "11222666000105" ~ "Delta",
    substr(VEN,1,1) == "1" & CNPJ_REV == "11222666000105" ~ "Delta",
    substr(VEN,1,1) == "1" & CNPJ_REV == "11222444000203" ~ "Beta Oeste",
    substr(VEN,1,1) == "2" & CNPJ_REV == "11222444000203" ~ "Beta Oeste",
    substr(VEN,1,1) %in% c("4","6","9") & CNPJ_REV == "11222444000203" ~ "Beta Oeste",
    substr(VEN,1,1) == "3" & CNPJ_REV == "11222444000203" ~ "Beta Oeste",
    substr(VEN,1,1) == "4" & CNPJ_REV == "11222444000102" ~ "Beta Leste",
    substr(VEN,1,1) %in% c("1","6","9") & CNPJ_REV == "11222444000102" ~ "Beta Leste",
    substr(VEN, 1, 1) == "9" & CNPJ_REV == "11222333000101" ~ "Alfa Linha B",
    substr(VEN, 1, 3) %in% c("148","149","150","151","152","154","156","158","160","171","173","248","249","270","936") & CNPJ_REV == "11222333000101" ~ "Alfa Linha A",
    substr(VEN, 1, 3) %in% c("153","155","159","161","162","163","164","170","172","174","250","252","253","269","271","272","273","348","349","350","836") & CNPJ_REV == "11222333000101" ~ "Alfa Linha B",
    TRUE ~ GRUPO_REV
  ))


livro_fiscal$CNPJ_REV = ifelse(substr(livro_fiscal$VEN,1,1) == "2" & livro_fiscal$CNPJ_REV == "11222999000209",
                               "11222999000310", livro_fiscal$CNPJ_REV)


livro_fiscal$MES_ATUAL = ifelse(format(as.Date(livro_fiscal$DATA, "%Y%m%d"),format = "%b/%Y") == format(Sys.Date(), format = "%b/%Y"), "Mês Atual",format(as.Date(livro_fiscal$DATA, "%Y%m%d"),format = "%b/%Y"))

livro_fiscal$PRODUTO_GENERICO = str_to_title(livro_fiscal$PRODUTO_GENERICO)
livro_fiscal$CHAVE_PROD = paste0(livro_fiscal$PRODUTO, livro_fiscal$CNPJ_REV)
livro_fiscal = left_join(livro_fiscal, produtos[,c("CHAVE_PROD", "UNIDADE")], by = "CHAVE_PROD")
livro_fiscal$QTD_REAL = floor(livro_fiscal$QTD/livro_fiscal$UNIDADE)
livro_fiscal = unique(livro_fiscal)


# Produtos categorizados --------------------------------------------------

produtos_cev$TIPO[str_detect(produtos_cev$PRODUTO, "CADEIRA|MESA|REFR|REF |CHOP")] = "Material"
produtos_cev$TIPO[str_detect(produtos_cev$PRODUTO, "CAIXA|GARRAFA")] = "Vasilhame"
produtos_cev$TIPO[!str_detect(produtos_cev$TIPO, "Vasilhame|Material")] = "Outro"
produtos_cev$TIPO[is.na(produtos_cev$TIPO)] = "Outro"


# Status de produtos ------------------------------------------------------

emprestimo_bloq = c('700301','700302','700303','700304','700305','700306') # Todos de Caixa 600ml sao bloqueados
caixa_1L = c('700101', '700102')
caixa_300ml = c('700201','700202','700203','700204','700205')
caixa_600ml_MARCA_A = c('700401','700402')

saldo_consolidado = livro_fiscal %>%
  filter(PRODUTO %in% c(caixa_1L, caixa_300ml, emprestimo_bloq, caixa_600ml_MARCA_A)) %>%
  group_by(CNPJ_REV) %>%
  summarise(`Recolha Ano 1L` = sum(QTD[OPERACAO == "Dev Emprestimo" & PRODUTO %in% c(caixa_1L)], na.rm = TRUE),
            `Empréstimo Ano 1L` = sum(QTD[OPERACAO == "Emprestei" & PRODUTO %in% c(caixa_1L)], na.rm = TRUE),
            `Recolha Ano 300ml` = sum(QTD[OPERACAO == "Dev Emprestimo" & PRODUTO %in% c(caixa_300ml)], na.rm = TRUE),
            `Empréstimo Ano 300ml` = sum(QTD[OPERACAO == "Emprestei" & PRODUTO %in% c(caixa_300ml)], na.rm = TRUE),
            `Recolha Ano 600ml` = sum(QTD[OPERACAO == "Dev Emprestimo" & PRODUTO %in% c(emprestimo_bloq)], na.rm = TRUE),
            `Empréstimo Ano 600ml` = sum(QTD[OPERACAO == "Emprestei" & PRODUTO %in% c(emprestimo_bloq)], na.rm = TRUE),
            `Recolha Ano 600ml MARCA_A` = sum(QTD[OPERACAO == "Dev Emprestimo" & PRODUTO %in% c(caixa_600ml_MARCA_A)], na.rm = TRUE),
            `Empréstimo Ano 600ml MARCA_A` = sum(QTD[OPERACAO == "Emprestei" & PRODUTO %in% c(caixa_600ml_MARCA_A)], na.rm = TRUE)) %>%
  mutate(`Caixa 1L` = `Recolha Ano 1L` - `Empréstimo Ano 1L`,
         `Caixa 300`= `Recolha Ano 300ml` - `Empréstimo Ano 300ml`,
         `Total MARCA_A 600`= `Recolha Ano 600ml MARCA_A` - `Empréstimo Ano 600ml MARCA_A` + floor((`Recolha Ano 600ml` - `Empréstimo Ano 600ml`)/5),
         `Caixa MARCA_A 600`= `Recolha Ano 600ml MARCA_A` - `Empréstimo Ano 600ml MARCA_A`,
         `Bônus MARCA_A 600`= floor((`Recolha Ano 600ml` - `Empréstimo Ano 600ml`)/5),
         `Caixa 600ml`= `Recolha Ano 600ml` - `Empréstimo Ano 600ml`) %>%
  select(CNPJ_REV,`Caixa 1L`,`Caixa 300`,`Total MARCA_A 600`,`Caixa MARCA_A 600`,`Bônus MARCA_A 600`,`Caixa 600ml`)

mapa_produtos <- data.frame(
  PRODUTO = c(caixa_1L, caixa_300ml, emprestimo_bloq, caixa_600ml_MARCA_A),
  TIPO = c(
    rep("Caixa 1L", length(caixa_1L)),
    rep("Caixa 300", length(caixa_300ml)),
    rep("Caixa 600ml", length(emprestimo_bloq)),
    rep("Caixa MARCA_A 600", length(caixa_600ml_MARCA_A))
  ),
  stringsAsFactors = FALSE
)

resultado_final <- mapa_produtos %>%
  crossing(CNPJ_REV = saldo_consolidado$CNPJ_REV) %>%
  left_join(saldo_consolidado, by = "CNPJ_REV") %>%
  mutate(
    saldo = case_when(
      TIPO == "Caixa 1L" ~ `Caixa 1L`,
      TIPO == "Caixa 300" ~ `Caixa 300`,
      TIPO == "Caixa 600ml" ~ `Caixa 600ml`,
      TIPO == "Caixa MARCA_A 600" ~ `Caixa MARCA_A 600`
    )
  ) %>%
  select(CNPJ_REV, PRODUTO, saldo)

resultado_final$STATUS_DLP = ifelse(resultado_final$saldo <= 0, "Bloqueado", "Disponível")
resultado_final$STATUS_DLP = ifelse(resultado_final$PRODUTO %in% emprestimo_bloq, "Bloqueado", resultado_final$STATUS_DLP)


produtos_cev = left_join(produtos_cev,
                         resultado_final[,c("CNPJ_REV", "PRODUTO", "STATUS_DLP")],
                         by = c("CNPJ_REV" = "CNPJ_REV", "CODCOMP" = "PRODUTO"))

# Clientes inadimplentes --------------------------------------------------

qtd_titulos = titulos_em_aberto %>%
  # filter(CODCLI == "10001") %>%
  group_by(CHAVE_CLI) %>%
  summarise(Titulos = sum(Inadimplente == "Sim", na.rm = TRUE))

clientes_inad = left_join(clientes[,c("CGC_CPF","PORTE","CHAVE","CHAVE_CLI","CHAVE_VEN","FANTASIA", "NOME", "CANAL", "VEN", "CODCLI", "CNPJ_REV",
                                      "ENDERECO", "CONTATO", "PTO_REF", "CIDADE", "DIA_VISIT", "SEQ", "CEP", "FONE","PROPRIETARIO",
                                      "CPF_PROP", "EMAIL_PROP","NASC_PROP")], titulos_agrupados[,c("CHAVE_CLI", "TITULO_MAIS_ANTIGO", "Inadimplente")], by = "CHAVE_CLI")
clientes_inad = left_join(clientes_inad, qtd_titulos, by = "CHAVE_CLI")
clientes_inad$CHAVE = str_pad(clientes_inad$CHAVE, width = 14, side = "left", pad = "0")
clientes_inad = left_join(clientes_inad, cev_cliente, by = "CHAVE_CLI")


clientes_inad = left_join(clientes_inad, funcionarios_externo[,c("CHAVE_VEN","subordinacao")],by = "CHAVE_VEN")

clientes_inad <- clientes_inad %>%
  mutate(across(where(is.character), ~ str_to_title(.)))

clientes_inad$CGC_CPF = ifelse(clientes_inad$CGC_CPF == "0000","CPF","CNPJ")

clientes_inad$CNPJ_REV = ifelse(substr(clientes_inad$VEN,1,1) %in% c("2") & clientes_inad$CNPJ_REV == "11222999000209",
                               "11222999000310", clientes_inad$CNPJ_REV)

clientes_inad = clientes_inad %>%
  mutate(REVENDA = case_when(
    CNPJ_REV == "11222333000101" ~ "Alfa",
    CNPJ_REV == "11222444000102" ~ "Beta Leste",
    CNPJ_REV == "11222444000203" ~ "Beta Oeste",
    CNPJ_REV == "11222555000104" ~ "Gama Norte",
    CNPJ_REV == "11222666000105" ~ "Delta",
    CNPJ_REV == "11222777000206" ~ "Epsilon",
    CNPJ_REV == "11222888000107" ~ "Gama Sul",
    CNPJ_REV == "11222888000208" ~ "Gama Centro",
    CNPJ_REV == "11222999000209" ~ "Zeta Polo 1",
    CNPJ_REV == "11222999000310" ~ "Zeta Polo 2",
    CNPJ_REV == "11222999000111" ~ "Zeta Polo 3",
    TRUE ~ NA_character_  # default
  ))

clientes_inad$Inadimplente[is.na(clientes_inad$Inadimplente)] = "Não"
clientes_inad$CEV_PEN = ifelse(is.na(clientes_inad$CEV_PEN),"Não","Sim")

# clientes_inad = clientes_inad %>% select(-Dia_Semana)

# pedidos$CHAVE_CLI = paste0(pedidos$CODCLI, pedidos$CNPJ_REV)

# Refrigeradores ----------------------------------------------------------

# cev_vencidos_a_vencer$CHAVE_CEVPROD = paste0(cev_vencidos_a_vencer$CEV, cev_vencidos_a_vencer$CHAVE_PROD)
refrigerador = cev_vencidos_a_vencer %>%
  filter(
    (str_detect(PRODUTO_GENERICO,"Ref")),
    !is.na(NOME)
  ) %>%
  group_by(CNPJ_REV, CODCLI, NOME, FANTASIA, VEN, CANAL, PORTE) %>%
  # group_by(CNPJ_REV, CODCLI, NOME, FANTASIA, VEN, CANAL, PORTE) %>%
  summarise(
    # `Data Comodato` = first(DTE),
    Total = sum(QTD,na.rm = TRUE),
    .groups = "drop"
  )
# Atualizar bases de dados ------------------------------------------------
# No servidor era a base do app de Comodatos; aqui dados/apps/comodatos/


if (nrow(cev)>0){

  # apaga a tabela se já existir e cria com os dados do dataframe
  gravar_tabela("apps/comodatos", "tb_comodato", cev)

}

if (nrow(livro_fiscal)>0){

  gravar_tabela("apps/comodatos", "tb_livro_fiscal", livro_fiscal)

}

if (nrow(clientes_inad)>0){

  gravar_tabela("apps/comodatos", "tb_clientes_inad", clientes_inad)

}

if (nrow(produtos_cev)>0){

  gravar_tabela("apps/comodatos", "tb_produtos_cev", produtos_cev)

}

# if (nrow(pedidos)>0){
#
#   gravar_tabela("apps/comodatos", "tb_pedidos_cev", pedidos)
#
# }

if (nrow(refrigerador)>0){

  gravar_tabela("apps/comodatos", "tb_refrig_cev", refrigerador)

}
