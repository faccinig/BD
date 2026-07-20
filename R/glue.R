#' Interpola valores em uma string SQL a partir de um data.frame
#'
#' @description
#' `glueData()` interpola colunas de um `data.frame` em uma instrução SQL
#' de forma segura, utilizando `glue::glue_data_sql()`. Cada linha do
#' `data.frame` gera uma instrução SQL independente, unidas por quebras
#' de linha (`\\n`).
#'
#' Esta função é útil para gerar múltiplas instruções SQL parametrizadas
#' a partir de dados tabulares, como inserções ou atualizações em lote.
#'
#' @param .data Um `data.frame` cujas colunas serão usadas na interpolação.
#' @param stmt Uma string com o template SQL, utilizando chaves `{}` para
#'   referenciar colunas de `.data`.
#' @param .which Apelido do banco de dados (opcional).
#' @param .path Caminho direto para o banco de dados (opcional).
#' @param .con Conexão DBI existente (opcional). Se fornecida, `.which` e
#'   `.path` são ignorados e a desconexão é responsabilidade do chamador.
#' @param .envir Ambiente onde as variáveis de `stmt` serão avaliadas.
#'   Padrão: `parent.frame()`.
#'
#' @returns Um objeto `SQL` contendo a instrução SQL interpolada. Pode
#'   ter comprimento maior que 1 quando `.data` possui múltiplas linhas.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#'
#' # Interpolar valores de um data.frame
#' parametros <- data.frame(
#'   nome = c("João", "Maria"),
#'   idade = c(30, 25)
#' )
#' sql <- glueData(parametros,
#'   "INSERT INTO pessoas (nome, idade) VALUES ({nome}, {idade})",
#'   .con = con)
#' print(sql)
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
glueData <- function(.data, stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  glue::glue_data_sql(.data, stmt, .sep = "\n", .con = .con, .envir = .envir)
}

#' Interpola valores em uma string SQL
#'
#' @description
#' `glue()` interpola variáveis do ambiente em uma instrução SQL de forma
#' segura, utilizando `glue::glue_sql()`. A função protege contra injeção
#' de SQL tratando adequadamente strings, números e valores nulos.
#'
#' Quando múltiplos templates são fornecidos, cada um gera uma instrução
#' SQL independente, unidas por quebras de linha (`\\n`).
#'
#' @param stmt Uma string (ou vetor de strings) com o template SQL,
#'   utilizando chaves `{}` para referenciar variáveis do ambiente.
#' @param .which Apelido do banco de dados (opcional).
#' @param .path Caminho direto para o banco de dados (opcional).
#' @param .con Conexão DBI existente (opcional). Se fornecida, `.which` e
#'   `.path` são ignorados e a desconexão é responsabilidade do chamador.
#' @param .envir Ambiente onde as variáveis de `stmt` serão avaliadas.
#'   Padrão: `parent.frame()`.
#'
#' @returns Um objeto `SQL` contendo a instrução SQL interpolada.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#'
#' # Interpolação simples
#' idade_min <- 18
#' sql <- glue("SELECT * FROM pessoas WHERE idade >= {idade_min}", .con = con)
#' print(sql)
#'
#' # Interpolação com string
#' nome <- "João"
#' sql <- glue("SELECT * FROM pessoas WHERE nome = {nome}", .con = con)
#' print(sql)
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
glue <- function(stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  glue::glue_sql(stmt, .sep = "\n", .con = .con, .envir = .envir)
}
