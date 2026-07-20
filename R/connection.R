#' Cria uma conexão com um banco de dados SQLite
#'
#' @description
#' `connection()` estabelece uma conexão DBI com um banco de dados SQLite,
#' utilizando `extended_types = TRUE` para suporte a tipos estendidos
#' (como datas e timestamps).
#'
#' A função resolve o caminho do banco através de `path()`, aceitando
#' tanto um apelido (`.which`) quanto um caminho direto (`.path`).
#'
#' @param .which Apelido do banco de dados, resolvido via `path()`.
#' @param .path Caminho direto para o arquivo do banco de dados. Deve
#'   possuir extensão `.sqlite`, `.sqlite3` ou `.db`.
#'
#' @returns Um objeto de conexão `SQLiteConnection` do pacote DBI.
#'
#' @examples
#' \dontrun{
#' # Conexão via caminho direto
#' con <- connection(.path = "meu_banco.sqlite3")
#' DBI::dbListTables(con)
#' DBI::dbDisconnect(con)
#'
#' # Conexão via apelido registrado
#' set_path(.path = "dados/principal.db", .which = "principal")
#' con <- connection(.which = "principal")
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
connection <- function(.which = NULL, .path = NULL) {
  .path <- path(.which, .path)
  if (is.null(.path)) stop("invalid path!")
  DBI::dbConnect(RSQLite::SQLite(), .path, extended_types = TRUE)
}
