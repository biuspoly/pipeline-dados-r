# Funções de apoio da versão pública --------------------------------------
#
# No servidor, a pipeline lia o ERP de cada revenda via ODBC e gravava as
# tabelas no MySQL/MariaDB. Nesta versão pública:
#   - as consultas ao ERP/MySQL de origem são respondidas por arquivos Parquet
#     fictícios em dados/origem/ (mesmas colunas que as consultas devolviam);
#   - as tabelas que eram gravadas no MySQL viram arquivos Parquet em
#     dados/bruto/ (camada bruta) e dados/apps/ (tabelas lidas pelos apps).
# As transformações de cada script foram mantidas.

library(nanoparquet)

if (!exists("raiz_pipeline")) raiz_pipeline = normalizePath(getwd())


# Credenciais -------------------------------------------------------------

# Ponto único para obter credenciais. Por padrão lê variável de ambiente;
# para usar um cofre (keyring, Vault, Secrets Manager...) basta trocar o corpo
# desta função. Nenhum valor fica no código.
obter_credencial = function(nome){
  Sys.getenv(nome, "")
}


# Caminhos ----------------------------------------------------------------

pasta_dados = function(...){
  file.path(raiz_pipeline, "dados", ...)
}


# Origem (substitui dbGetQuery no ERP / MySQL) ----------------------------

# Equivale a dbGetQuery(con, sql) no banco do ERP da revenda.
# O 'sql' fica só como registro da consulta original.
consulta_erp = function(banco, consulta, sql = NULL){
  arquivo = file.path(raiz_pipeline, "dados", "origem", "erp", banco, paste0(consulta, ".parquet"))
  if (!file.exists(arquivo)) return(data.frame())
  as.data.frame(nanoparquet::read_parquet(arquivo))
}

# Equivale a dbGetQuery(con, sql) num banco MySQL que não é gerado pela pipeline
consulta_mysql = function(base, consulta, sql = NULL){
  arquivo = file.path(raiz_pipeline, "dados", "origem", "mysql", base, paste0(consulta, ".parquet"))
  if (!file.exists(arquivo)) return(data.frame())
  as.data.frame(nanoparquet::read_parquet(arquivo))
}


# Tabelas geradas (substitui dbExistsTable/dbRemoveTable/dbCreateTable/dbAppendTable)

existe_tabela = function(base, tabela){
  file.exists(pasta_dados(base, paste0(tabela, ".parquet")))
}

ler_tabela = function(base, tabela){
  as.data.frame(nanoparquet::read_parquet(pasta_dados(base, paste0(tabela, ".parquet"))))
}

# apaga a tabela se já existir e cria com os dados do dataframe
gravar_tabela = function(base, tabela, df){
  dir.create(pasta_dados(base), recursive = TRUE, showWarnings = FALSE)
  nanoparquet::write_parquet(as.data.frame(df), pasta_dados(base, paste0(tabela, ".parquet")))
}

# acrescenta linhas numa tabela existente (ou cria)
anexar_tabela = function(base, tabela, df){
  if (existe_tabela(base, tabela)) {
    df = rbind(ler_tabela(base, tabela), as.data.frame(df))
  }
  gravar_tabela(base, tabela, df)
}
