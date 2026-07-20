#' Executa uma consulta SQL e retorna os resultados
#'
#' @description
#' `GetQuery()` combina `glue()` e `DBI::dbGetQuery()` para executar uma
#' consulta SQL com interpolação segura de variáveis e retornar os
#' resultados em uma `tibble`.
#'
#' Esta é a principal função de consulta do pacote. Use-a sempre que
#' precisar obter dados do banco com interpolação de variáveis do R.
#'
#' @param stmt Uma string com a consulta SQL, utilizando chaves `{}`
#'   para interpolar variáveis do ambiente.
#' @param .which Apelido do banco de dados (opcional).
#' @param .path Caminho direto para o banco de dados (opcional).
#' @param .con Conexão DBI existente (opcional). Se fornecida, `.which` e
#'   `.path` são ignorados e a desconexão é responsabilidade do chamador.
#' @param .envir Ambiente onde as variáveis de `stmt` serão avaliadas.
#'   Padrão: `parent.frame()`.
#'
#' @returns Uma `tibble` com os resultados da consulta.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#'
#' # Criar tabela de exemplo
#' DBI::dbWriteTable(con, "mtcars", mtcars)
#'
#' # Consulta com interpolação
#' cilindros <- 4
#' resultado <- GetQuery(
#'   "SELECT mpg, cyl, hp FROM mtcars WHERE cyl = {cilindros}",
#'   .con = con)
#' print(resultado)
#'
#' # Consulta simples
#' total <- GetQuery("SELECT count(*) AS n FROM mtcars", .con = con)
#' print(total)
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
GetQuery <- function(stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  sql <- glue(stmt, .con = .con, .envir = .envir)

  tibble::as_tibble(DBI::dbGetQuery(.con, sql))
}

#' Executa consultas SQL parametrizadas a partir de um data.frame
#'
#' @description
#' `GetQueryData()` combina `glueData()` e `GetQuery()` para executar uma
#' ou mais consultas SQL cujos parâmetros vêm das colunas de um `data.frame`.
#'
#' Cada linha de `.data` gera uma consulta independente, cujos resultados
#' são combinados com `rbind()` em uma única `tibble`. Se `.data` tiver
#' apenas uma linha, o resultado é idêntico ao de `GetQuery()`.
#'
#' @param .data Um `data.frame` cujas colunas serão usadas como parâmetros
#'   na interpolação SQL.
#' @param stmt Uma string com o template SQL, utilizando chaves `{}` para
#'   referenciar colunas de `.data`.
#' @param .which Apelido do banco de dados (opcional).
#' @param .path Caminho direto para o banco de dados (opcional).
#' @param .con Conexão DBI existente (opcional). Se fornecida, `.which` e
#'   `.path` são ignorados e a desconexão é responsabilidade do chamador.
#' @param .envir Ambiente onde variáveis adicionais de `stmt` serão
#'   avaliadas. Padrão: `parent.frame()`.
#'
#' @returns Uma `tibble` com os resultados combinados de todas as consultas.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#' DBI::dbWriteTable(con, "mtcars", mtcars)
#'
#' # Consultar para múltiplos valores de cilindros
#' params <- data.frame(cil = c(4, 6))
#' resultado <- GetQueryData(params,
#'   "SELECT mpg, cyl FROM mtcars WHERE cyl = {cil}",
#'   .con = con)
#' print(resultado)
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
GetQueryData <- function(.data, stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  sql <- glueData(.data, stmt, .con = .con, .envir = .envir)
  if (length(sql) == 1L) return(GetQuery(sql, .con = .con))

  tbls <- lapply(sql, GetQuery, .con = .con)
  tbls <- do.call(rbind, tbls)

  tibble::as_tibble(tbls)
}
