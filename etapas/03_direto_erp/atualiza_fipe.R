library(httr)
library(jsonlite)
library(glue)
library(stringr)
library(stringdist)
# Configurações do banco --------------------------------------------------
# Versão pública: MySQL substituído por Parquet (ver R/funcoes_pipeline.R).
# O token da API FIPE vem de obter_credencial("FIPE_TOKEN"); sem ele o
# rodar_pipeline.R pula esta etapa.



# Listar tipos e modelos --------------------------------------------------

tipos_modelos = consulta_mysql("frotas", "cadsen_tipos_modelos", "
                           SELECT DISTINCT
                                CASE
                                    WHEN TIPO LIKE '%CAMINHAO%' THEN 'CAMINHAO'
                                    WHEN TIPO LIKE '%CAMIONETA%' THEN 'AUTOMOVEL'
                                    WHEN TIPO = 'UTILITARIO' THEN 'AUTOMOVEL'
                                    WHEN TIPO = 'CAMINHONETE' THEN 'AUTOMOVEL'
                                    ELSE TIPO
                                END AS TIPO,

                                REPLACE(
                                    CASE
                                        WHEN SUBSTRING_INDEX(MODELO, '/', 1) = 'VW' AND TIPO LIKE '%CAMINHAO%' THEN 'VOLKSWAGEN'
                                        WHEN SUBSTRING_INDEX(MODELO, '/', 1) = 'VW' AND TIPO NOT LIKE '%CAMINHAO%' THEN 'VW - VOLKSWAGEN'
                                        WHEN SUBSTRING_INDEX(MODELO, '/', 1) = 'CHEVROLET' AND TIPO IN ('AUTOMOVEL', 'UTILITARIO', 'CAMINHONETE') THEN 'GM - CHEVROLET'
                                        WHEN SUBSTRING_INDEX(MODELO, '/', 1) = 'CHEV' AND TIPO IN ('AUTOMOVEL', 'UTILITARIO', 'CAMINHONETE') THEN 'GM - CHEVROLET'
                                        WHEN SUBSTRING_INDEX(MODELO, '/', 1) = 'I' THEN SUBSTRING_INDEX(MODELO, ' ', 1)
                                        WHEN SUBSTRING_INDEX(MODELO, '/', 1) = 'M.BENZ' THEN 'MERCEDES-BENZ'
                                        WHEN SUBSTRING_INDEX(MODELO, '/', 1) = 'CITROEN' THEN 'CITROËN'
                                        ELSE SUBSTRING_INDEX(MODELO, '/', 1)
                                    END,
                                    'I/', ''
                                ) AS MARCA,

                                CASE
                                    WHEN modelo_final LIKE '%CONSTELLATION%'
                                    THEN REPLACE(modelo_final, '.', '-')
                                    ELSE modelo_final
                                END AS MODELO,

                                ANO

                            FROM (
                                SELECT
                                    TIPO,
                                    MODELO,
                                    ANO,

                                    REPLACE(
                                        REPLACE(
                                            CASE
                                                WHEN SUBSTRING_INDEX(MODELO, '/', 1) = 'I'
                                                    THEN SUBSTRING(MODELO, LOCATE(' ', MODELO) + 1)
                                                ELSE SUBSTRING_INDEX(MODELO, '/', -1)
                                            END,
                                            'CNM',
                                            'CONSTEL'
                                        ),
                                        'CRM',
                                        'CONSTEL'
                                    ) AS modelo_final

                                FROM cadsen
                                WHERE TIPO NOT LIKE '%REBOQUE%'
                            ) x;
                           ")


tabela_fipe = data.frame()

for(linha in 1:nrow(tipos_modelos)){

  variavel_tipo = switch(
    tipos_modelos$TIPO[linha],
    "CAMINHAO"  = "trucks",
    "AUTOMOVEL" = "cars",
    "motorcycles"
  )


  response_tipo_veiculo = GET(glue("https://fipe.parallelum.com.br/api/v2/{variavel_tipo}/brands"),
                              add_headers(
                                "accept" = "application/json",
                                "content-type" = "application/json",
                                "X-Subscription-Token" = obter_credencial("FIPE_TOKEN")
                              ))

  if(response_tipo_veiculo$status_code == 200){
    tabela_tipo = fromJSON(content(response_tipo_veiculo, "text", encoding = "UTF-8"))

    codigo_marca = tabela_tipo$code[toupper(tabela_tipo$name) == tipos_modelos$MARCA[linha]]

    if(length(codigo_marca) > 0){
      response_marca_veiculo = GET(glue("https://fipe.parallelum.com.br/api/v2/{variavel_tipo}/brands/{codigo_marca}/models"),
                                   add_headers(
                                     "accept" = "application/json",
                                     "content-type" = "application/json",
                                     "X-Subscription-Token" = obter_credencial("FIPE_TOKEN")
                                   ))

      if(response_marca_veiculo$status_code == 200){
        tabela_marca = fromJSON(content(response_marca_veiculo, "text", encoding = "UTF-8"))

        for(prob in seq(0.1,1,0.1)){

          pos_agrep = agrep(tipos_modelos$MODELO[linha], toupper(tabela_marca$name),max.distance = prob)
          pos_sub <- which(str_detect(toupper(tabela_marca$name), glue("({tipos_modelos$MODELO[linha]}|{tipos_modelos$MODELO[linha]}/)")))
          pos_sub_2 <- which(str_detect(toupper(tabela_marca$name), glue("({tipos_modelos$MODELO[linha]}/)")))

          candidatos = Reduce(intersect, Filter(length, list(pos_agrep, pos_sub, pos_sub_2)))[1]

          if(length(candidatos) == 0){
            candidatos = unique(c(pos_agrep, pos_sub, pos_sub_2))[1]
          }

          codigo_modelo_provavel = tabela_marca$code[candidatos]

          if(!is.na(codigo_modelo_provavel)){


            if(str_detect(toupper(tabela_marca$name[agrep(tipos_modelos$MODELO[linha], toupper(tabela_marca$name),max.distance = prob)][1]), "DIESEL")){
              variavel_comb = "3"
            } else if(str_detect(toupper(tabela_marca$name[agrep(tipos_modelos$MODELO[linha], toupper(tabela_marca$name),max.distance = prob)][1]), "ALCOOL")){
              variavel_comb = "2"
            } else{
              variavel_comb = "1"
            }

            break
          }


        }


        response_model_veiculo = GET(glue("https://fipe.parallelum.com.br/api/v2/{variavel_tipo}/brands/{codigo_marca}/models/{codigo_modelo_provavel}/years/{tipos_modelos$ANO[linha]}-{variavel_comb}"),
                                     add_headers(
                                       "accept" = "application/json",
                                       "content-type" = "application/json",
                                       "X-Subscription-Token" = obter_credencial("FIPE_TOKEN")
                                     ))

        if(response_model_veiculo$status_code == 200){
          tabela_model = as.data.frame(fromJSON(content(response_model_veiculo, "text", encoding = "UTF-8")))
          tabela_model$codigo_marca = codigo_marca
          tabela_model$codigo_modelo = codigo_modelo_provavel

          tabela_fipe = rbind(tabela_fipe, tabela_model[,c("price","codeFipe","referenceMonth","fuel","codigo_marca","codigo_modelo")])
        } else{
          response_model_veiculo = GET(glue("https://fipe.parallelum.com.br/api/v2/{variavel_tipo}/brands/{codigo_marca}/models/{codigo_modelo_provavel}/years/"),
                                       add_headers(
                                         "accept" = "application/json",
                                         "content-type" = "application/json",
                                         "X-Subscription-Token" = obter_credencial("FIPE_TOKEN")
                                       ))

          if(response_model_veiculo$status_code == 200){

            tabela_ano = as.data.frame(fromJSON(content(response_model_veiculo, "text", encoding = "UTF-8")))$code[1]

            response_model_veiculo = GET(glue("https://fipe.parallelum.com.br/api/v2/{variavel_tipo}/brands/{codigo_marca}/models/{codigo_modelo_provavel}/years/{tabela_ano}"),
                                         add_headers(
                                           "accept" = "application/json",
                                           "content-type" = "application/json",
                                           "X-Subscription-Token" = obter_credencial("FIPE_TOKEN")
                                         ))

            if(response_model_veiculo$status_code == 200){
              tabela_model = as.data.frame(fromJSON(content(response_model_veiculo, "text", encoding = "UTF-8")))
              tabela_model$codigo_marca = codigo_marca
              tabela_model$codigo_modelo = codigo_modelo_provavel

              tabela_fipe = rbind(tabela_fipe, tabela_model[,c("price","codeFipe","referenceMonth","fuel","codigo_marca","codigo_modelo")])
            } else{
              tabela_fipe <- rbind(tabela_fipe, data.frame(price = NA, codeFipe = NA, referenceMonth = NA, fuel = NA, codigo_marca = NA, codigo_modelo = NA))
            }

          } else{
            tabela_fipe <- rbind(tabela_fipe, data.frame(price = NA, codeFipe = NA, referenceMonth = NA, fuel = NA, codigo_marca = NA, codigo_modelo = NA))
          }

        }

      } else{
        tabela_fipe <- rbind(tabela_fipe, data.frame(price = NA, codeFipe = NA, referenceMonth = NA, fuel = NA, codigo_marca = NA, codigo_modelo = NA))
      }
    } else{
      tabela_fipe <- rbind(tabela_fipe, data.frame(price = NA, codeFipe = NA, referenceMonth = NA, fuel = NA, codigo_marca = NA, codigo_modelo = NA))
    }


  } else{
    tabela_fipe <- rbind(tabela_fipe, data.frame(price = NA, codeFipe = NA, referenceMonth = NA, fuel = NA, codigo_marca = NA, codigo_modelo = NA))
  }


}

tipos_modelos = cbind(tipos_modelos, tabela_fipe)

if (nrow(tipos_modelos)>0){

  # apaga a tabela se já existir e cria com os dados do dataframe
  gravar_tabela("apps/frotas", "tb_fipe_cadsen", tipos_modelos)

}
