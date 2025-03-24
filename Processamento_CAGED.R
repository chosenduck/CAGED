## PREPARAÇÃO ==================================================================
### Limpando Environment e Memória =============================================
rm(list=ls(all=T))
gc()

### Bibliotecas ================================================================
if (!require(pacman))
  install.packages("pacman")

require(pacman)

p_load(tidyverse, dplyr, plyr, data.table, janitor, openxlsx, readxl, rio, 
       skimr, vroom, lubridate, stringi, arrow, tidyr, stringr, purrr)

### Ajustando diretórios =======================================================
getwd()
wd <-  setwd("...")

diretorio_brutos <- paste0(wd, "/dados_brutos/")
diretorio_processados <- paste0(wd, "/dados_processados/")
diretorio_auxiliares <- paste0(wd, "/arquivos_auxiliares/")

### Abrindo Dicionários ========================================================
dicionarios <- lapply(
  excel_sheets(paste0(diretorio_auxiliares, "dicionários.xlsx")
    ), 
  function(sheet) {
    read_excel(paste0(diretorio_auxiliares, "dicionários.xlsx"), 
               sheet = sheet)
    }
  )

dic_classes <- dicionarios[[1]]
dic_faixaeta <- dicionarios[[2]]
dic_escolaridade <- dicionarios[[3]]
dic_sexo <- dicionarios[[4]]
dic_racacor <- dicionarios[[5]]
dic_local <- dicionarios[[6]]

rm(dicionarios)

# PRÉ - PROCESSAMENTO ==========================================================
### Conversão .txt para .rds ===================================================

# Listando arquivos .txt
list_arc <- list.files(path = diretorio_brutos, pattern = ".txt", 
                       full.names = TRUE)

# Loop para conversão
if (length(list_arc) != 0) {
  for (i in list_arc) {
    dados <- fread(i, stringsAsFactors = TRUE, encoding = "UTF-8")
    arquivo_rds <- paste0(diretorio_processados, gsub(".txt", ".rds", basename(i)))
    saveRDS(dados, arquivo_rds)
    }
  rm(dados, i)
}

# PROCESSAMENTO ================================================================
#### Parâmetros ================================================================
tipos_arquivos <- c(
  "CAGEDEXC", 
  "CAGEDFOR", 
  "CAGEDMOV2020", 
  "CAGEDMOV2021", 
  "CAGEDMOV2022", 
  "CAGEDMOV2023", 
  "CAGEDMOV2024",
  "CAGEDMOV2025"
  )

colunas_selecionadas <- c(
  "competenciamov", "regiao", "uf",
  "subclasse","sexo", "idade", "graudeinstrucao", "racacor", "salario", 
  "admissoes", "desligamentos", "saldomovimentacao"
)

salario_minimo <- c('2020' = 1045, 
                    '2021' = 1100, 
                    '2022' = 1212, 
                    '2023' = 1302, 
                    '2024' = 1412,
                    '2025' = 1518
                    )

filtro_uf = 31

#### Funções ===================================================================
# Adicionar coluna de faixa etária
adicionar_faixaeta <- function(idade) {
  case_when(
    idade >= 10 & idade <= 14 ~ 1,
    idade >= 15 & idade <= 17 ~ 2,
    idade >= 18 & idade <= 24 ~ 3,
    idade >= 25 & idade <= 29 ~ 4,
    idade >= 30 & idade <= 39 ~ 5,
    idade >= 40 & idade <= 49 ~ 6,
    idade >= 50 & idade < 64 ~ 7,
    idade > 65 ~ 8,
    TRUE ~ 99
  )
}

# Adicionar coluna de região
adicionar_regiao <- function(regiao) {
  case_when(
    regiao == 1 ~ "Norte",
    regiao == 2 ~ "Nordeste",
    regiao == 3 ~ "Sudeste",
    regiao == 4 ~ "Sul",
    regiao == 5 ~ "Centro-Oeste",
    regiao == 9 ~ "Não Identificado",
    TRUE ~ NA_character_
  )
}

# Adicionar coluna de faixa salarial
calcular_faixa_salarial <- function(salario, ano, salario_minimo) {
  case_when(
    salario <= salario_minimo[as.character(ano)] ~ "Até 1 salário mínimo",
    salario > salario_minimo[as.character(ano)] & salario <= 2 * salario_minimo[as.character(ano)] ~ "Entre 1 e 2 salários mínimos",
    salario > 2 * salario_minimo[as.character(ano)] & salario <= 3 * salario_minimo[as.character(ano)] ~ "Entre 2 e 3 salários mínimos",
    salario > 3 * salario_minimo[as.character(ano)] & salario <= 4 * salario_minimo[as.character(ano)] ~ "Entre 3 e 4 salários mínimos",
    salario > 4 * salario_minimo[as.character(ano)] & salario <= 5 * salario_minimo[as.character(ano)] ~ "Entre 4 e 5 salários mínimos",
    salario > 5 * salario_minimo[as.character(ano)] & salario <= 10 * salario_minimo[as.character(ano)] ~ "Entre 5 e 10 salários mínimos",
    salario > 10 * salario_minimo[as.character(ano)] ~ "Mais de 10 salários mínimos",
    TRUE ~ NA_character_
  )
}

# Função para formatar o tempo de execução
format_time <- function(seconds) {
  hours <- floor(seconds / 3600)
  minutes <- floor((seconds %% 3600) / 60)
  seconds <- round(seconds %% 60, 2)
  if (hours > 0) {
    return(paste(hours, "hora(s),", minutes, "minuto(s) e", seconds, "segundo(s)"))
  } else if (minutes > 0) {
    return(paste(minutes, "minuto(s) e", seconds, "segundo(s)"))
  } else {
    return(paste(seconds, "segundo(s)"))
  }
}

# Função de processamento para cada tipo de arquivo
processar_arquivo <- function(
    tipo_arquivo, 
    diretorio_processados, 
    colunas_selecionadas, 
    salario_minimo, 
    dic_classes, 
    dic_faixaeta, 
    dic_escolaridade, 
    dic_sexo, 
    dic_racacor, 
    dic_local
) {
  
  # Captura de tempo
  start_time <- Sys.time()
  
  # Ajuste do padrão de identificação de arquivos
  if (grepl("^CAGEDMOV\\d{4}$", tipo_arquivo)) {
    
    # Se for CAGEDMOV seguido do ano (ex: CAGEDMOV2021), busca arquivos mensais desse ano
    pattern_arquivos <- paste0(tipo_arquivo, "\\d{2}\\.rds$")
  } else {
    
    # Para CAGEDEXC e CAGEDFOR, mantém o padrão fixo
    pattern_arquivos <- paste0(tipo_arquivo, "\\d{6}\\.rds$")
  }
  
  # Busca os arquivos com o padrão correto
  arquivos <- list.files(path = diretorio_processados, 
                         pattern = pattern_arquivos, 
                         full.names = TRUE)
  
  if (length(arquivos) == 0) {
    cat("\nNenhum arquivo encontrado para:", tipo_arquivo, "\n")
    return(NULL)
  }
  
  cat("\nProcessando:", tipo_arquivo, "(", length(arquivos), "arquivos )\n")
  
  df_tipo <- NULL
  
  for (arquivo in arquivos) {
    cat("\nLendo:", arquivo)
    data <- readRDS(arquivo)
    
    if (is.null(data)) {
      cat("\nFalha ao ler o arquivo:", arquivo, "\n")
      next
    }
    
    # Renomeando as colunas para minúsculas e removendo acentos
    data <- data %>%
      rename_with(~ tolower(stri_trans_general(., "Latin-ASCII"))) %>% 
      
    # Aplicando o filtro de UF
      filter(uf == filtro_uf)
    
    # Admissões, desligamentos e saldo de movimentação
    if (tipo_arquivo == "CAGEDEXC") {
      data <- data %>%
        mutate(
          admissoes = ifelse(saldomovimentacao == 1, -1, 0),
          desligamentos = ifelse(saldomovimentacao == -1, -1, 0),
          saldomovimentacao = ifelse(saldomovimentacao == 1, -1, 1)
        )
    } else {
      data <- data %>%
        mutate(
          admissoes = ifelse(saldomovimentacao == 1, 1, 0),
          desligamentos = ifelse(saldomovimentacao == -1, 1, 0)
        )
    }
    
    # Transformação da coluna salario
    data <- data %>%
      mutate(
        salario = as.factor(salario))
    
    # Verificação se df_tipo está NULL
    if (is.null(df_tipo)) {
      df_tipo <- data
    } else {
      df_tipo <- rbindlist(list(df_tipo, data), use.names = TRUE, fill = TRUE)
    }
  }
  
  # Agora, fazemos a seleção das colunas apenas se df_tipo não for NULL
  if (!is.null(df_tipo)) {
    df_tipo <- df_tipo %>%
      select(all_of(colunas_selecionadas)) %>% 
      mutate(
        ano = substr(competenciamov, 1, 4),
        competenciamov = paste0(substr(competenciamov, 1, 4),
                                substr(competenciamov, 5, 6)),
        competenciamov = as.Date(competenciamov, format = "%Y%m"),
        salario = as.numeric(str_replace_na(str_replace(as.character(salario), ",", "."), "0")),
        faixa_salarial = calcular_faixa_salarial(salario, ano, salario_minimo),
        sexo = mapvalues(sexo, from = dic_sexo$cod, to = dic_sexo$nom, warn_missing = FALSE),
        racacor = mapvalues(racacor, from = dic_racacor$cod, to = dic_racacor$nom, warn_missing = FALSE),
        graudeinstrucao = mapvalues(graudeinstrucao, from = dic_escolaridade$cod, to = dic_escolaridade$nom, warn_missing = FALSE),
        faixaetaria = adicionar_faixaeta(idade),
        faixaeta = mapvalues(faixaetaria, from = dic_faixaeta$cod, to = dic_faixaeta$nom, warn_missing = FALSE),
        subclasse = mapvalues(subclasse, from = dic_classes$Subclasse, to = dic_classes$`Nome Subclasse`, warn_missing = FALSE),
        classe = mapvalues(subclasse, from = dic_classes$Subclasse, to = dic_classes$`Nome Classe`, warn_missing = FALSE),
        secao = mapvalues(subclasse, from = dic_classes$Subclasse, to = dic_classes$`Nome Seção`, warn_missing = FALSE),
        uf = mapvalues(uf, from = dic_local$UF, to = dic_local$Nome_UF, warn_missing = FALSE),
        regiao = adicionar_regiao(regiao)
        ) %>% 
      select(-faixaetaria)
  
    # Agregando os dados
    data <- data %>%
      group_by_all() %>%
      mutate(
        admissoes = sum(admissoes),
        desligamentos = sum(desligamentos),
        saldomovimentacao = sum(saldomovimentacao)
      )
    
    # Processamento incremental
    if (is.null(df_tipo)) {
      df_tipo <- data
    } else {
      df_tipo <- rbindlist(list(df_tipo, data), use.names = TRUE, fill = TRUE)
      }}
  
  parquet_path <- paste0(diretorio_processados, tipo_arquivo, ".parquet")
  write_parquet(df_tipo, parquet_path)
  
  # Salvar como arquivo Parquet por tipo
  processing_time <- Sys.time() - start_time
  
  cat("\nArquivo processado:", tipo_arquivo, "\nTempo de execução:", format_time(as.numeric(processing_time, units = "secs")))
  
  return(invisible(df_tipo))
  
  }

### Processando arquivos =======================================================
map(tipos_arquivos, ~processar_arquivo(
  tipo_arquivo = .x, 
  diretorio_processados = diretorio_processados, 
  colunas_selecionadas = colunas_selecionadas, 
  salario_minimo = salario_minimo, 
  dic_classes = dic_classes, 
  dic_faixaeta = dic_faixaeta, 
  dic_escolaridade = dic_escolaridade, 
  dic_sexo = dic_sexo, 
  dic_racacor = dic_racacor, 
  dic_local = dic_local
))


### Gerando CAGED_UNIFICADA ====================================================
dfs <- list()

# Loop para carregar os arquivos
for (arquivo in tipos_arquivos) {
  caminho_arquivo <- file.path(diretorio_processados, paste0(arquivo, ".parquet"))
  
  if (file.exists(caminho_arquivo)) {
    tryCatch({
      df <- read_parquet(caminho_arquivo)
      cat(arquivo, "lido com sucesso!\n")
      dfs[[arquivo]] <- df 
      
      rm(df)  
      gc()  
      
    }, error = function(e) {
      cat("Erro ao ler", arquivo, ":", conditionMessage(e), "\n")
    })
  }
  else {
    cat("Arquivo", arquivo, "não encontrado!\n")
  }
}


# Verifica se algum arquivo foi carregado
if (length(dfs) > 0) {
  
  # Concatenando os dataframes carregados
  CAGED_UNIFICADA <- bind_rows(dfs)
  
  # Definição de colunas de soma e agrupamento
  colunas_soma <- c("admissoes", "desligamentos", "saldomovimentacao")
  colunas_agrupamento <- setdiff(names(CAGED_UNIFICADA), colunas_soma)
  
  # Convertendo colunas para tipo numérico inteiro
  CAGED_UNIFICADA <- CAGED_UNIFICADA %>%
    mutate(across(all_of(colunas_soma), as.integer))
  
  # Agrupando e somando as colunas numéricas
  CAGED_AGRUPADO <- CAGED_UNIFICADA %>%
    group_by(across(all_of(colunas_agrupamento))) %>%
    summarise(across(all_of(colunas_soma), sum, .groups = "drop"))
  
  # Salvando arquivo
  write_parquet(CAGED_AGRUPADO, file.path(diretorio_processados, "CAGED_UNIFICADA.parquet"), compression = "snappy")
  
  cat("CAGED_UNIFICADA criada com sucesso!\n")
} else {
  cat("Nenhum arquivo foi carregado. Verifique os arquivos de entrada.\n")
}
