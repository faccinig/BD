#' Executa instruções SQL parametrizadas a partir de um data.frame
#'
#' @description
#' `ExecuteData()` combina `glueData()` e `DBI::dbExecute()` para executar
#' instruções SQL (INSERT, UPDATE, DELETE, CREATE, etc.) cujos parâmetros
#' vêm das colunas de um `data.frame`.
#'
#' Cada linha de `.data` gera uma instrução independente. O valor de
#' retorno é a soma do número de linhas afetadas por todas as instruções.
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
#' @returns Um inteiro com o total de linhas afetadas por todas as
#'   instruções executadas.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#' DBI::dbExecute(con, "CREATE TABLE t (x INTEGER, y TEXT)")
#'
#' # Inserir múltiplas linhas de uma vez
#' dados <- data.frame(
#'   valor = c(10, 20, 30),
#'   rotulo = c("dez", "vinte", "trinta"),
#'   stringsAsFactors = FALSE
#' )
#' ExecuteData(dados,
#'   "INSERT INTO t (x, y) VALUES ({valor}, {rotulo})",
#'   .con = con)
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
ExecuteData <- function(.data, stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  sql <- glueData(.data, stmt, .con = .con, .envir = .envir)
  res <- vapply(sql, function(statement) DBI::dbExecute(.con, statement), integer(1L))

  sum(res)
}

#' Executa uma instrução SQL
#'
#' @description
#' `Execute()` combina `glue()` e `DBI::dbExecute()` para executar uma
#' instrução SQL (INSERT, UPDATE, DELETE, CREATE, DROP, etc.) com
#' interpolação segura de variáveis.
#'
#' Use esta função para comandos que modificam o banco de dados, como
#' inserções, atualizações e criação de tabelas. Para consultas que
#' retornam dados, use `GetQuery()`.
#'
#' @param stmt Uma string com a instrução SQL, utilizando chaves `{}`
#'   para interpolar variáveis do ambiente.
#' @param .which Apelido do banco de dados (opcional).
#' @param .path Caminho direto para o banco de dados (opcional).
#' @param .con Conexão DBI existente (opcional). Se fornecida, `.which` e
#'   `.path` são ignorados e a desconexão é responsabilidade do chamador.
#' @param .envir Ambiente onde as variáveis de `stmt` serão avaliadas.
#'   Padrão: `parent.frame()`.
#'
#' @returns Um inteiro com o número de linhas afetadas pela instrução.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#'
#' # Criar uma tabela
#' Execute("CREATE TABLE produtos (id INTEGER, nome TEXT, preco REAL)",
#'         .con = con)
#'
#' # Inserir dados com interpolação
#' produto <- "Caneta"
#' valor <- 2.50
#' Execute(
#'   "INSERT INTO produtos (nome, preco) VALUES ({produto}, {valor})",
#'   .con = con)
#'
#' # Atualizar dados
#' Execute("UPDATE produtos SET preco = 3.00 WHERE nome = 'Caneta'",
#'         .con = con)
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
Execute <- function(stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  sql <- glue(stmt, .con = .con, .envir = .envir)
  res <- vapply(sql, function(statement) DBI::dbExecute(.con, statement), integer(1L))

  sum(res)
}
