# Gera os arquivos Parquet "de origem" com dados FICTÍCIOS ----------------
#
# Cada arquivo tem as mesmas colunas que a consulta original devolvia
# (ERP via ODBC ou MySQL). Nada aqui veio de dados reais: revendas, CNPJs,
# clientes, placas, pessoas e valores são inventados.
#
# Uso (a partir da raiz do projeto):  Rscript dados/gerar_dados_ficticios.R
# As datas são geradas em relação a Sys.Date(), para os filtros de período
# dos scripts sempre encontrarem dados.

library(nanoparquet)

if (!exists("raiz_pipeline")) raiz_pipeline = normalizePath(getwd())

set.seed(2026)
hoje = Sys.Date()
ymd_num = function(d) as.numeric(format(d, "%Y%m%d"))

gravar = function(df, ...){
  arquivo = file.path(raiz_pipeline, "dados", "origem", ...)
  dir.create(dirname(arquivo), recursive = TRUE, showWarnings = FALSE)
  nanoparquet::write_parquet(df, arquivo)
}

# Revendas fictícias ------------------------------------------------------

revendas = data.frame(
  cnpj  = c("11222333000101","11222444000102","11222444000203","11222555000104",
            "11222666000105","11222777000206","11222888000107","11222888000208",
            "11222999000209","11222999000310","11222999000111"),
  banco = c("ERP_ALFA","ERP_BETA_LESTE","ERP_BETA_OESTE","ERP_GAMA_NORTE",
            "ERP_DELTA","ERP_EPSILON","ERP_GAMA_SUL","ERP_GAMA_CENTRO",
            "ERP_ZETA_1","ERP_ZETA_2","ERP_ZETA_3"),
  licenca = 1001:1011,
  stringsAsFactors = FALSE
)

# Catálogo de produtos (igual em todas as revendas) -----------------------

catalogo = data.frame(
  CODCOMP = c("101001","101002","101003","101004","101005",
              "700101","700203","700306","700402","700501","700502","700503"),
  CODRED  = c(800001, 800002, 900100, 900101, 900102,
              900200, 900201, 900202, 900203, 900204, 900205, 900206),
  PRODUTO = c("CERVEJA LATA 350ML 12UN","CERVEJA MARCA X 269ML 15UN","CERVEJA GARRAFA 600ML 24UN",
              "REFRIGERANTE PET 2L 6UN","AGUA MINERAL 500ML 12UN",
              "CAIXA PLASTICA 1L 12UN","CAIXA PLASTICA 300ML 24UN","GARRAFA 600ML 24UN",
              "CAIXA 600ML ESPECIAL 24UN","REFRIGERADOR 1 PORTA 1UN","MESA PLASTICA 1UN","CADEIRA PLASTICA 1UN"),
  PRODUTO_GENERICO = c("CERVEJA LATA 350ML","CERVEJA MARCA X 269ML","CERVEJA 600ML",
                       "REFRIGERANTE 2L","AGUA 500ML",
                       "CAIXA PLASTICA 1L","CAIXA PLASTICA 300ML","GARRAFA 600ML",
                       "CAIXA 600ML ESPECIAL","REFRIGERADOR 1 PORTA","MESA PLASTICA","CADEIRA PLASTICA"),
  UNIDADE = c(12, 15, 24, 6, 12, 12, 24, 24, 24, 1, 1, 1),
  TIPO    = c("CERVEJA","CERVEJA","CERVEJA","REFRIGERANTE","AGUA",
              "VASILHAME","VASILHAME","VASILHAME","VASILHAME","MATERIAL","MATERIAL","MATERIAL"),
  CEV     = c(rep("N", 5), rep("S", 7)),
  stringsAsFactors = FALSE
)
vendaveis = catalogo[catalogo$CEV == "N", ]
preco_base = c(45, 52, 160, 38, 18)

vendedores_cod = c("138","148","153","238","338","438","640","738")
motoristas_cod = c("801","802","803","804")

# Uma revenda por vez -----------------------------------------------------

for (i in seq_len(nrow(revendas))) {

  banco = revendas$banco[i]
  n_cli = 24

  ## Clientes
  codcli = as.character(10000 + seq_len(n_cli))
  cpf = seq_len(n_cli) %% 3 == 0            # 1/3 dos clientes são pessoa física
  clientes = data.frame(
    CGC_CPF = ifelse(cpf, "0000", sprintf("%04d", 1000 + seq_len(n_cli))),
    FANTASIA = sprintf("BAR FICTICIO %03d", seq_len(n_cli)),
    CHAVE = as.numeric(sprintf("90%03d%03d%06d", i, seq_len(n_cli), 1)),
    CANAL = sample(c("BAR","MERCEARIA","RESTAURANTE","ADEGA"), n_cli, TRUE),
    COD_CATEGORIA = sample(c("A1","B3","K1","L2","E7","Z9"), n_cli, TRUE),
    CIDADE = sample(c("CIDADE A","CIDADE B","CIDADE C"), n_cli, TRUE),
    CODCLI = codcli,
    PORTE = sample(c("Diamante","Ouro","Prata","Bronze"), n_cli, TRUE),
    ACRESCIMO = 1.4,
    VEN = sample(vendedores_cod, n_cli, TRUE),
    NOME = sprintf("CLIENTE FICTICIO %03d LTDA", seq_len(n_cli)),
    TPCOBRANCA = sample(c(1L, 4L), n_cli, TRUE),
    CPG = 1L,
    BANCO = 0L,
    LIMITE_CREDITO = round(runif(n_cli, 1000, 20000)),
    ENDERECO = sprintf("RUA FICTICIA, %d - CENTRO - CIDADE A - XX", seq_len(n_cli)),
    CONTATO = sprintf("CONTATO %03d", seq_len(n_cli)),
    PTO_REF = "",
    DIA_VISIT = as.character(sample(1:6, n_cli, TRUE)),
    SEQ = as.character(seq_len(n_cli)),
    CEP = "00000000",
    FONE = "0000000000",
    PROPRIETARIO = sprintf("PROPRIETARIO FICTICIO %03d", seq_len(n_cli)),
    CPF_PROP = NA_real_,
    NASC_PROP = 19800101,
    EMAIL_PROP = NA_character_,
    QUINZENAL = 1L,
    stringsAsFactors = FALSE
  )
  gravar(clientes, "erp", banco, "clientes.parquet")

  ## Produtos e preços
  gravar(catalogo, "erp", banco, "produtos.parquet")
  gravar(data.frame(CODCOMP = catalogo$CODCOMP,
                    PRECO_UN = round(runif(nrow(catalogo), 2, 30), 2),
                    PRECO = round(runif(nrow(catalogo), 30, 300), 2),
                    TAB_CEV = paste0(format(hoje, "%Y%m"), "99")),
         "erp", banco, "preco_prod.parquet")

  ## Vendedores e motoristas
  gravar(data.frame(COD = vendedores_cod, NOME = paste("VENDEDOR FICTICIO", vendedores_cod)),
         "erp", banco, "vendedores.parquet")
  gravar(data.frame(COD = motoristas_cod, NOME = paste("MOTORISTA FICTICIO", motoristas_cod)),
         "erp", banco, "motoristas.parquet")

  ## Romaneios (um por dia útil dos últimos 6 meses + hoje/ontem)
  dias = seq(hoje - 180, hoje, by = "day")
  dias = dias[format(dias, "%u") != "7"]
  rom = data.frame(
    ROMANEIO = paste0(format(dias, "%Y%m%d"), "001"),
    DATA = ifelse(dias >= hoje - 3, 0, ymd_num(dias + 1)),   # 0 = romaneio ainda aberto
    MOTORISTA = sample(motoristas_cod, length(dias), TRUE),
    stringsAsFactors = FALSE
  )
  gravar(rom, "erp", banco, "romaneios.parquet")

  ## Pedidos, vendas/devoluções e itens
  n_ped = 220
  dia_ped = sample(dias, n_ped, TRUE)
  dia_ped[1:25] = sample(dias[dias >= hoje - 3], 25, TRUE)   # alguns em romaneios abertos
  cli_ped = sample(seq_len(n_cli), n_ped, TRUE)
  cod_ped = as.numeric(format(dia_ped, "%Y%m%d")) * 10000 + seq_len(n_ped)
  romaneio_ped = as.numeric(paste0(format(dia_ped, "%Y%m%d"), "001"))
  status = sample(c(0, 8, 2, 9), n_ped, TRUE, prob = c(0.6, 0.2, 0.1, 0.1))
  oco = sample(c(1, 60, 50), n_ped, TRUE, prob = c(0.8, 0.15, 0.05))

  itens = do.call(rbind, lapply(seq_len(n_ped), function(p){
    k = sample(1:3, 1)
    prods = sample(seq_len(nrow(vendaveis)), k)
    # alguns pedidos com bonificação da lata 350ml sem a Marca X vendida
    cfop = ifelse(p %% 7 == 0 & vendaveis$CODRED[prods] == 800001, 5910, 5102)
    qtd = sample(1:20, k, TRUE)
    dev = ifelse(runif(k) < 0.15, sample(1:3, k, TRUE), 0)
    data.frame(p = p, CODR = vendaveis$CODRED[prods], PRD_DESCRICAO_GENERICA = vendaveis$PRODUTO_GENERICO[prods],
               CFOP = cfop, QTD = qtd, DEVOLUCAO_CX = pmin(dev, qtd),
               VALOR_VENDA = qtd * preco_base[prods], VALOR_DEVOLUCAO = pmin(dev, qtd) * preco_base[prods])
  }))
  # pedidos grandes para clientes pessoa física (gera casos de limite de CPF)
  grandes = itens$p %in% which(cpf[cli_ped])[1:6]
  itens$VALOR_VENDA[grandes] = itens$VALOR_VENDA[grandes] * 40

  vend_dev_itens = data.frame(
    COD_PED = cod_ped[itens$p], CODR = itens$CODR, PRD_DESCRICAO_GENERICA = itens$PRD_DESCRICAO_GENERICA,
    CFOP = itens$CFOP, QTD = itens$QTD, DEVOLUCAO_CX = itens$DEVOLUCAO_CX,
    `VALOR VENDA` = itens$VALOR_VENDA, `VALOR DEVOLUCAO` = itens$VALOR_DEVOLUCAO,
    check.names = FALSE
  )
  gravar(vend_dev_itens, "erp", banco, "vend_dev_itens.parquet")

  vendas_e_dev = data.frame(
    COD_PED = cod_ped[itens$p],
    CODR = itens$CODR,
    VEN = clientes$VEN[cli_ped[itens$p]],
    MOT = rom$MOTORISTA[match(romaneio_ped[itens$p], as.numeric(rom$ROMANEIO))],
    CLI = as.numeric(codcli[cli_ped[itens$p]]),
    MDV_DESCRICAO = ifelse(itens$VALOR_DEVOLUCAO > 0,
                            sample(c("CLIENTE FECHADO","SEM DINHEIRO","PEDIDO EM DUPLICIDADE","AVARIA"), nrow(itens), TRUE),
                            NA_character_),
    CMP_TIPO = ifelse(runif(nrow(itens)) < 0.05, "C", NA_character_),
    TPCOBRA = ifelse(clientes$TPCOBRANCA[cli_ped[itens$p]] == 4, "Prazo", "A Vista"),
    NF_VENDA = 50000 + itens$p,
    ROMANEIO = romaneio_ped[itens$p],
    QTD = itens$QTD,
    DEVOLUCAO_CX = itens$DEVOLUCAO_CX,
    QTD_UN = 0,
    DEVOLUCAO_UN = 0,
    `VALOR VENDA` = itens$VALOR_VENDA,
    `VALOR DEVOLUCAO` = itens$VALOR_DEVOLUCAO,
    MES = format(dia_ped[itens$p], "%Y%m"),
    DATA_VENDA = format(dia_ped[itens$p], "%Y%m%d"),
    DATA_DEV = ifelse(itens$VALOR_DEVOLUCAO > 0, ymd_num(dia_ped[itens$p] + 1), 0),
    NF_DEV = ifelse(itens$VALOR_DEVOLUCAO > 0, 70000 + itens$p, 0),
    check.names = FALSE, stringsAsFactors = FALSE
  )
  gravar(vendas_e_dev, "erp", banco, "vendas_e_dev.parquet")

  valor_ped = tapply(itens$VALOR_VENDA, itens$p, sum)
  pedidos = data.frame(
    PEDIDO = cod_ped,
    DT = ymd_num(dia_ped),
    ROMANEIO = romaneio_ped,
    CODCLI = as.numeric(codcli[cli_ped]),
    COD_VENDEDOR = clientes$VEN[cli_ped],
    TIPO_COBRANCA = clientes$TPCOBRANCA[cli_ped],
    OCO = oco,
    PED_ORIGEM = sample(c("ERP","Portal","SFA","Loja Parceira"), n_ped, TRUE),
    NOTA_FISCAL = 50000 + seq_len(n_ped),
    VALOR = as.numeric(valor_ped[as.character(seq_len(n_ped))]),
    CHAVE_NFE = sprintf("%044.0f", 1e6 * i + seq_len(n_ped)),   # chave de NF-e fictícia (zeros + sequencial)
    VENC = ymd_num(dia_ped + 28),
    STATUS = status,
    FORA_ROTA = "D",
    REPROGRAMADO = "",
    PAG = ifelse(status == 8, format(dia_ped + 28, "%Y%m%d"), NA_character_),
    stringsAsFactors = FALSE
  )
  gravar(pedidos, "erp", banco, "pedidos.parquet")

  ## Comodatos (CEV), títulos, estoque e livro fiscal
  n_cev = 30
  dte = sample(seq(hoje - 300, hoje - 1, by = "day"), n_cev, TRUE)
  prod_cev = sample(catalogo$CODCOMP[catalogo$CEV == "S"], n_cev, TRUE)
  cev = data.frame(
    CODCLI = sample(codcli, n_cev, TRUE),
    CODCOMP = as.numeric(prod_cev),
    CEV = 3000 + seq_len(n_cev),
    MOT = sample(motoristas_cod, n_cev, TRUE),
    DTE = ymd_num(dte),
    DTV = ymd_num(dte + sample(c(10, 30, 180, 365), n_cev, TRUE)),
    QTD = sample(1:30, n_cev, TRUE),
    NF = 60000 + seq_len(n_cev),
    SERIE = "1",
    NPED = format(as.numeric(format(dte, "%Y%m%d")) * 10000 + 9000 + seq_len(n_cev), scientific = FALSE),
    PRE_PED = 0,
    ROMA = as.numeric(paste0(format(dte, "%Y%m%d"), "001")),
    STATUS = "PENDENTE",
    stringsAsFactors = FALSE
  )
  gravar(cev, "erp", banco, "cev.parquet")

  gravar(data.frame(CODCLI = sample(codcli, 15, TRUE),
                    VEN = sample(vendedores_cod, 15, TRUE),
                    TITULO_MAIS_ANTIGO = ymd_num(hoje - sample(-10:60, 15, TRUE)),
                    stringsAsFactors = FALSE),
         "erp", banco, "titulos_abertos.parquet")

  gravar(data.frame(CODCOMP = catalogo$CODCOMP[catalogo$CEV == "S"][1:5], ESTOQUE = "SIM"),
         "erp", banco, "estoque.parquet")

  vasilhames = c("700101","700203","700306","700402")
  n_lf = 40
  prod_lf = sample(vasilhames, n_lf, TRUE)
  dia_lf = sample(seq(as.Date(format(hoje, "%Y-%m-01")), hoje, by = "day"), n_lf, TRUE)
  base_lf = data.frame(
    NF = 80000 + seq_len(n_lf),
    CLI = as.numeric(sample(codcli, n_lf, TRUE)),
    PRODUTO = as.numeric(prod_lf),
    PRODUTO_GENERICO = catalogo$PRODUTO_GENERICO[match(prod_lf, catalogo$CODCOMP)],
    QTD = sample(12:240, n_lf, TRUE),
    QTDS = 0,
    VEN = sample(vendedores_cod, n_lf, TRUE),
    stringsAsFactors = FALSE
  )
  entrada = base_lf[1:20, ]
  entrada = data.frame(NF = entrada$NF, OPERACAO = sample(c("Dev Venda","Dev Emprestimo"), 20, TRUE),
                       DATA = ymd_num(dia_lf[1:20]), entrada[, c("CLI","PRODUTO","PRODUTO_GENERICO","QTD","QTDS","VEN")])
  saida = base_lf[21:40, ]
  saida = data.frame(NF = saida$NF, OPERACAO = sample(c("Vendi","Emprestei"), 20, TRUE),
                     DATA = format(dia_lf[21:40], "%Y%m%d"), saida[, c("CLI","PRODUTO","PRODUTO_GENERICO","QTD","QTDS","VEN")])
  gravar(entrada, "erp", banco, "livro_entrada_cev.parquet")
  gravar(saida, "erp", banco, "livro_saida_cev.parquet")

  ## PDE: romaneios de ontem e hoje com os clientes atendidos
  dias_pde = c(hoje - 1, hoje)
  pde = do.call(rbind, lapply(seq_along(dias_pde), function(d){
    clis = c(as.numeric(sample(codcli, 5)), 900000 + (i %in% 2:3) * (i - 1))  # inclui cliente de transferência
    data.frame(
      ROMANEIO = paste0(format(dias_pde[d], "%Y%m%d"), "00", d),
      PLACA = sprintf("FIC-%03d", i * 10 + d),
      MOTORISTA = sample(motoristas_cod, 1),
      ROTA = sample(c("ROTA CENTRO","ROTA NORTE (A)","ROTA SUL - B"), 1),
      CAPACIDADE = 8000,
      PESO_ROMANEIO = round(runif(1, 3000, 7900)),
      QTD_PEDIDOS = length(clis),
      QTD_ENT = 1,
      DATA = ifelse(d == 1, ymd_num(hoje), 0),
      FAT_SAIDA = round(runif(length(clis), 300, 3000), 2),
      QTD_DEV = rbinom(length(clis), 1, 0.15),
      FAT_DEV = 0,
      HORA_ENT = sample(15:19, 1),
      MINUTO_ENT = sample(0:59, 1),
      KM_SAIDA = 10000 + 100 * i,
      KM_CHEGADA = 10000 + 100 * i + sample(40:180, 1),
      CLIENTE = clis,
      stringsAsFactors = FALSE
    )
  }))
  pde$FAT_DEV = ifelse(pde$QTD_DEV > 0, pde$FAT_SAIDA, 0)
  gravar(pde, "erp", banco, "romaneio_pde.parquet")

  ## Último romaneio por placa (app de Frotas)
  placas = sprintf("FIC-%03d", i * 10 + 1:3)
  gravar(data.frame(ULT_ROMANEIO = paste0(format(hoje - sample(0:20, 3), "%Y%m%d"), "001"),
                    PLACA = placas, stringsAsFactors = FALSE),
         "erp", banco, "ultimo_romaneio.parquet")
}

# Bases MySQL que não são geradas pela pipeline ---------------------------

## Cadastro de funcionários (banco externo)
gravar(data.frame(
  id = seq_len(nrow(revendas) * 3),
  nome = sprintf("FUNCIONARIO FICTICIO %02d", seq_len(nrow(revendas) * 3)),
  licenca = rep(revendas$licenca, each = 3),
  rca = rep(c("138","148","238"), nrow(revendas)),
  subordinacao = rep(c("SUPERVISOR A","SUPERVISOR B","SUPERVISOR C"), nrow(revendas)),
  stringsAsFactors = FALSE
), "mysql", "externo", "funcionarios.parquet")

## Resultado da consulta de tipos/modelos da frota (cadsen)
gravar(data.frame(
  TIPO   = c("CAMINHAO","CAMINHAO","AUTOMOVEL","MOTOCICLETA"),
  MARCA  = c("VOLKSWAGEN","MERCEDES-BENZ","FIAT","HONDA"),
  MODELO = c("DELIVERY 11","ACCELO 1016","STRADA","CG 160"),
  ANO    = c(2020, 2019, 2022, 2021),
  stringsAsFactors = FALSE
), "mysql", "frotas", "cadsen_tipos_modelos.parquet")

message("Dados fictícios gerados em ", file.path(raiz_pipeline, "dados", "origem"))
