# Pipeline de dados - execução completa -----------------------------------
#
# No servidor, estes scripts rodavam de madrugada, agendados, para que os
# apps Shiny abrissem de manhã com os dados do dia anterior.
# Ordem: 1) camada bruta (ERP -> bruto); 2) tabelas dos apps a partir da
# camada bruta; 3) tabelas que vêm direto do ERP (PDE e Frotas).
#
# Uso:  Rscript rodar_pipeline.R      (de qualquer diretório)

inicio_pipeline = Sys.time()

argumentos = commandArgs(trailingOnly = FALSE)
arquivo_script = sub("^--file=", "", argumentos[grep("^--file=", argumentos)])
raiz_pipeline = if (length(arquivo_script) == 1) normalizePath(dirname(arquivo_script)) else normalizePath(getwd())
setwd(raiz_pipeline)

source("R/funcoes_pipeline.R", encoding = "UTF-8")

# Dados de origem fictícios (já versionados; recria se faltarem)
if (!dir.exists(pasta_dados("origem", "erp"))) {
  source("dados/gerar_dados_ficticios.R", encoding = "UTF-8")
}

etapas = c(
  # 1. Camada bruta
  "etapas/01_camada_bruta/atualiza_dados_brutos.R",
  "etapas/01_camada_bruta/atualiza_comodatos_bruto.R",
  # 2. Tabelas dos apps a partir da camada bruta
  "etapas/02_tabelas_apps/atualiza_devolucoes.R",
  "etapas/02_tabelas_apps/atualiza_romaneios.R",
  "etapas/02_tabelas_apps/atualiza_comodato.R",
  "etapas/02_tabelas_apps/atualiza_pedidos_canc.R",
  # 3. Direto do ERP
  "etapas/03_direto_erp/atualiza_PDE.R",
  "etapas/03_direto_erp/atualiza_romaneio.R"
)

# A etapa da FIPE consulta uma API externa e precisa de token
if (obter_credencial("FIPE_TOKEN") != "") {
  etapas = c(etapas, "etapas/03_direto_erp/atualiza_fipe.R")
} else {
  message("FIPE_TOKEN não definido: etapa atualiza_fipe.R não será executada.")
}

for (etapa in etapas) {
  message("\n==> ", etapa)
  inicio_etapa = Sys.time()
  source(etapa, local = new.env(parent = globalenv()), encoding = "UTF-8")
  message(sprintf("    ok (%.1f s)", as.numeric(difftime(Sys.time(), inicio_etapa, units = "secs"))))
}

saidas = list.files(pasta_dados(), pattern = "\\.parquet$", recursive = TRUE)
saidas = saidas[!startsWith(saidas, "origem/")]
message("\nTabelas geradas (", length(saidas), "):")
message(paste0("  dados/", saidas, collapse = "\n"))
message(sprintf("\nPipeline concluída em %.1f s", as.numeric(difftime(Sys.time(), inicio_pipeline, units = "secs"))))
