# Path -------------------------------------------------------------------------

#' Resolve o caminho de um banco de dados
#'
#' @description
#' `path()` resolve o caminho completo de um banco de dados a partir de um
#' apelido ou de um caminho direto fornecido pelo usuário.
#'
#' A resolução segue a seguinte ordem de prioridade:
#' 1. Se `.path` for informado diretamente, retorna-o após validar a extensão.
#' 2. Se `.which` corresponder a um apelido registrado via `set_path()`,
#'    retorna o caminho armazenado na sessão.
#' 3. Se `.which` corresponder a uma entrada em `config::get("BD")` no
#'    arquivo `config.yml`, retorna o caminho definido na configuração.
#' 4. Caso contrário, retorna `NULL`.
#'
#' @param .which Uma string com o apelido do banco de dados. Quando `NULL`,
#'   utiliza `"default"`. Geralmente usado em conjunto com um arquivo
#'   `config.yml` ou com `set_path()`.
#' @param .path Caminho direto para o arquivo do banco de dados (opcional).
#'   Deve possuir uma extensão válida: `.sqlite`, `.sqlite3` ou `.db`.
#'
#' @returns Uma string com o caminho resolvido do banco de dados, ou `NULL`
#'   se o apelido não for encontrado.
#'
#' @examples
#' # Caminho direto com extensão válida
#' path(.path = "meu_banco.sqlite3")
#'
#' # Apelido registrado via set_path()
#' set_path(.path = "dados/vendas.db", .which = "vendas")
#' path(.which = "vendas")
#'
#' # Apelido padrão
#' set_path(.path = "principal.sqlite", .which = "default")
#' path()
#'
#' # Apelido inexistente retorna NULL (sem config.yml)
#' clear_paths()
#' path(.which = "inexistente")
#'
#' @export
path <- function(.which = NULL, .path = NULL) {
  if (!is.null(.path)) {
    stopifnot("`.path` must be a character!" = is.character(.path),
              "`.path` must have length 1!" = length(.path) == 1L,
              "`.path` doesn't have a valid extension!" = !is.null(BD_type(.path))
              )
    return(.path)
  }

  if (is.null(.which)) .which <- "default"
  stopifnot("`.which` must be a character!" = is.character(.which),
            "`.which` must have length 1!" = length(.which) == 1L)

  paths <- get_paths()

  if (.which %in% names(paths)) return(paths[[.which]])

  config_path <- config::get("BD")
  if (!is.null(config_path) && .which %in% names(config_path)) return(config_path[[.which]])

  NULL
}


#' Registra um apelido para o caminho de um banco de dados
#'
#' @description
#' `set_path()` associa um apelido (`.which`) a um caminho de banco de dados
#' (`.path`), armazenando-o na sessão atual do R. O apelido pode ser usado
#' posteriormente por qualquer função do pacote que aceite o parâmetro `.which`.
#'
#' O registro é volátil: dura apenas durante a sessão atual do R e não é
#' persistido em disco. Para configurações permanentes, utilize o arquivo
#' `config.yml`.
#'
#' @param .path Caminho para o arquivo do banco de dados. Deve possuir uma
#'   extensão reconhecida: `.sqlite`, `.sqlite3` ou `.db`.
#' @param .which Uma string com o apelido a ser associado ao caminho.
#'   O valor padrão é `"default"`.
#'
#' @returns `NULL`, invisivelmente.
#'
#' @examples
#' # Registrar um banco de dados com apelido
#' set_path(.path = "dados/financeiro.sqlite3", .which = "financeiro")
#'
#' # Usar o apelido padrão
#' set_path(.path = "principal.db")
#'
#' # Verificar se foi registrado
#' path(.which = "financeiro")
#'
#' @export
set_path <- function(.path = NULL, .which = "default") {

  stopifnot("`.path` must be defined!" = !is.null(.path),
            "`.path` must be a character!" = is.character(.path),
            "`.path` must have length 1!" = length(.path) == 1L)

  if (is.null(BD_type(.path))) {
    ext <- tolower(tools::file_ext(.path))
    stop(paste0("extension `", ext, "` not suported!"))
  }

  stopifnot("`.which` must be a character!" = is.character(.which),
            "`.which` must have length 1!" = length(.which) == 1L)

  paths <- get_paths()
  paths[[.which]] <- .path
  set_paths(paths)

  invisible()
}

#' Remove todos os apelidos de bancos de dados da sessão
#'
#' @description
#' `clear_paths()` remove todos os apelidos de caminhos de bancos de dados
#' que foram registrados via `set_path()` durante a sessão atual do R.
#'
#' Esta função não afeta as configurações definidas no arquivo `config.yml`.
#'
#' @returns `NULL`, invisivelmente.
#'
#' @examples
#' # Registrar alguns apelidos
#' set_path(.path = "banco_a.sqlite", .which = "a")
#' set_path(.path = "banco_b.sqlite", .which = "b")
#'
#' # Listar apelidos ativos
#' list_paths()
#'
#' # Limpar todos
#' clear_paths()
#'
#' # Agora apenas os apelidos do config.yml (se existir) permanecem
#' list_paths()
#'
#' @export
clear_paths <- function() {
  set_paths(list())
  invisible()
}

#' Lista todos os bancos de dados configurados
#'
#' @description
#' `list_paths()` retorna uma lista combinando os apelidos definidos no
#' arquivo `config.yml` (via `config::get("BD")`) com os apelidos
#' registrados dinamicamente via `set_path()` durante a sessão.
#'
#' Apelidos registrados via `set_path()` sobrescrevem entradas do
#' `config.yml` com o mesmo nome.
#'
#' @returns Uma lista nomeada onde cada elemento é o caminho de um banco
#'   de dados associado ao seu apelido.
#'
#' @examples
#' # Registrar apelidos na sessão
#' set_path(.path = "producao.db", .which = "producao")
#'
#' # Listar todos os apelidos disponíveis
#' list_paths()
#'
#' # Limpar e verificar novamente
#' clear_paths()
#' list_paths()
#'
#' @export
list_paths <- function() {
  lst_paths <- tryCatch(config::get("BD"),
                        error = identity)
  if (is.null(lst_paths) || inherits(lst_paths, "error")) lst_paths <- list()

  paths <- get_paths()
  if (length(paths) > 0L) {
    nms <- names(paths)
    for (i in seq_along(paths)) {
      lst_paths[[nms[i]]] <- paths[[i]]
    }
  }
  lst_paths
}

BD_type <- function(.path) {
  switch (tolower(tools::file_ext(.path)),
          "sqlite" = "lite",
          "sqlite3" = "lite",
          "db" = "lite")
}

# Connection -------------------------------------------------------------------


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


# db ----


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

#' Lê uma tabela inteira do banco de dados
#'
#' @description
#' `ReadTable()` lê todas as linhas e colunas de uma tabela do banco de
#' dados e as retorna como um `data.frame`.
#'
#' Equivale a `DBI::dbReadTable()`, com a conveniência adicional de
#' aceitar os parâmetros `.which`, `.path` e `.con` para especificar
#' a conexão.
#'
#' @param name Nome da tabela a ser lida.
#' @param .which Apelido do banco de dados (opcional).
#' @param .path Caminho direto para o banco de dados (opcional).
#' @param .con Conexão DBI existente (opcional). Se fornecida, `.which` e
#'   `.path` são ignorados e a desconexão é responsabilidade do chamador.
#'
#' @returns Um `data.frame` com o conteúdo completo da tabela.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#' DBI::dbWriteTable(con, "iris", iris)
#'
#' # Ler a tabela inteira
#' dados <- ReadTable("iris", .con = con)
#' head(dados)
#'
#' # Também funciona com .path
#' dados <- ReadTable("iris", .path = "banco.sqlite")
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
ReadTable <- function(name, .which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbReadTable(.con, name)
}

#' Escreve dados em uma tabela do banco de dados
#'
#' @description
#' `WriteTable()` escreve um `data.frame` em uma tabela do banco de dados.
#' Por padrão, faz `append = TRUE` (adiciona linhas a uma tabela existente).
#'
#' Equivale a `DBI::dbWriteTable()`, com a conveniência adicional de
#' aceitar os parâmetros `.which`, `.path` e `.con`. Para sobrescrever a
#' tabela, use `append = FALSE` ou a função `OverwriteTable()`.
#'
#' @param value Um `data.frame` com os dados a serem escritos.
#' @param name Nome da tabela de destino.
#' @param .which Apelido do banco de dados (opcional).
#' @param .path Caminho direto para o banco de dados (opcional).
#' @param .con Conexão DBI existente (opcional). Se fornecida, `.which` e
#'   `.path` são ignorados e a desconexão é responsabilidade do chamador.
#' @param append Lógico. Se `TRUE` (padrão), adiciona linhas à tabela
#'   existente. Se `FALSE`, sobrescreve a tabela completamente.
#' @param fields Vetor nomeado com os tipos SQL das colunas (opcional).
#'   Ex: `c(id = "INTEGER PRIMARY KEY", nome = "TEXT")`. Passado como
#'   `field.types` para `DBI::dbWriteTable()`.
#'
#' @returns `NULL`, invisivelmente.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#'
#' # Escrever dados pela primeira vez
#' WriteTable(mtcars, "carros", .con = con)
#'
#' # Adicionar mais linhas (append = TRUE é o padrão)
#' WriteTable(mtcars[1:5, ], "carros", .con = con)
#'
#' # Sobrescrever a tabela
#' WriteTable(iris, "carros", .con = con, append = FALSE)
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
WriteTable <- function(value, name,
                       .which = NULL, .path = NULL, .con = NULL,
                       append = TRUE, fields = NULL) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  overwrite <- !append

  DBI::dbWriteTable(.con, name, value,
                    append = append ,
                    overwrite = overwrite,
                    field.types = fields)
}

#' Cria uma tabela vazia no banco de dados
#'
#' @description
#' `CreateTable()` cria uma tabela vazia no banco de dados com a estrutura
#' de colunas especificada. Equivale a `DBI::dbCreateTable()`.
#'
#' @param name Nome da tabela a ser criada.
#' @param fields Vetor nomeado com os tipos SQL das colunas.
#'   Ex: `c(id = "INTEGER", nome = "TEXT", valor = "REAL")`.
#' @param .which Apelido do banco de dados (opcional).
#' @param .path Caminho direto para o banco de dados (opcional).
#' @param .con Conexão DBI existente (opcional). Se fornecida, `.which` e
#'   `.path` são ignorados e a desconexão é responsabilidade do chamador.
#'
#' @returns `NULL`, invisivelmente.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#'
#' # Criar uma tabela com estrutura definida
#' CreateTable("clientes",
#'   c(id = "INTEGER PRIMARY KEY",
#'     nome = "TEXT",
#'     email = "TEXT",
#'     idade = "INTEGER"),
#'   .con = con)
#'
#' # Verificar que a tabela existe vazia
#' ExistsTable("clientes", .con = con)
#' ReadTable("clientes", .con = con)
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
CreateTable <- function(name, fields,
                        .which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbCreateTable(.con, name = name, fields = fields)
}

#' Adiciona linhas a uma tabela existente
#'
#' @description
#' `AppendTable()` é um atalho para `WriteTable()` com `append = TRUE`.
#' Adiciona as linhas do `data.frame` ao final de uma tabela já existente
#' no banco de dados.
#'
#' @param value Um `data.frame` com os dados a serem adicionados.
#' @param name Nome da tabela de destino.
#' @param .which Apelido do banco de dados (opcional).
#' @param .path Caminho direto para o banco de dados (opcional).
#' @param .con Conexão DBI existente (opcional). Se fornecida, `.which` e
#'   `.path` são ignorados e a desconexão é responsabilidade do chamador.
#'
#' @returns `NULL`, invisivelmente.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#'
#' # Criar tabela inicial
#' WriteTable(iris[1:50, ], "flores", .con = con)
#'
#' # Adicionar mais linhas
#' AppendTable(iris[51:100, ], "flores", .con = con)
#' AppendTable(iris[101:150, ], "flores", .con = con)
#'
#' # Verificar total
#' nrow(ReadTable("flores", .con = con))
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
AppendTable <- function(value, name, .which = NULL, .path = NULL, .con = NULL) {
  WriteTable(value, name, append = TRUE,.which = .which, .path = .path, .con = .con)
}

#' Sobrescreve uma tabela no banco de dados
#'
#' @description
#' `OverwriteTable()` é um atalho para `WriteTable()` com `append = FALSE`.
#' Remove a tabela existente (se houver) e a recria com os novos dados.
#'
#' Use com cuidado: todos os dados anteriores da tabela serão perdidos.
#'
#' @param value Um `data.frame` com os dados a serem escritos.
#' @param name Nome da tabela de destino.
#' @param .which Apelido do banco de dados (opcional).
#' @param .path Caminho direto para o banco de dados (opcional).
#' @param .con Conexão DBI existente (opcional). Se fornecida, `.which` e
#'   `.path` são ignorados e a desconexão é responsabilidade do chamador.
#' @param fields Vetor nomeado com os tipos SQL das colunas (opcional).
#'   Ex: `c(id = "INTEGER PRIMARY KEY", nome = "TEXT")`.
#'
#' @returns `NULL`, invisivelmente.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#'
#' # Escrever dados iniciais
#' WriteTable(mtcars, "dados", .con = con)
#'
#' # Substituir completamente por novos dados
#' OverwriteTable(iris, "dados", .con = con)
#'
#' # Verificar que os dados foram substituídos
#' nrow(ReadTable("dados", .con = con))
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
OverwriteTable <- function(value, name,
                           .which = NULL, .path = NULL, .con = NULL,
                           fields = NULL) {
  WriteTable(value, name, append = FALSE,
             .which = .which, .path = .path, .con = .con,
             fields = fields)
}

#' Remove uma tabela do banco de dados
#'
#' @description
#' `RemoveTable()` exclui permanentemente uma tabela do banco de dados.
#' Equivale a `DBI::dbRemoveTable()`. Esta operação não pode ser desfeita.
#'
#' @param name Nome da tabela a ser removida.
#' @param .which Apelido do banco de dados (opcional).
#' @param .path Caminho direto para o banco de dados (opcional).
#' @param .con Conexão DBI existente (opcional). Se fornecida, `.which` e
#'   `.path` são ignorados e a desconexão é responsabilidade do chamador.
#'
#' @returns `NULL`, invisivelmente.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#' DBI::dbWriteTable(con, "temporaria", mtcars)
#'
#' # Remover a tabela
#' RemoveTable("temporaria", .con = con)
#'
#' # Verificar que foi removida
#' ExistsTable("temporaria", .con = con)
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
RemoveTable <- function(name, .which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbRemoveTable(.con,name)
}

#' Verifica se uma tabela existe no banco de dados
#'
#' @description
#' `ExistsTable()` verifica a existência de uma tabela no banco de dados.
#' Retorna `TRUE` se a tabela existir, `FALSE` caso contrário.
#'
#' @param name Nome da tabela a ser verificada.
#' @param .which Apelido do banco de dados (opcional).
#' @param .path Caminho direto para o banco de dados (opcional).
#' @param .con Conexão DBI existente (opcional). Se fornecida, `.which` e
#'   `.path` são ignorados e a desconexão é responsabilidade do chamador.
#'
#' @returns `TRUE` se a tabela existir, `FALSE` caso contrário.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#'
#' # Verificar antes de criar
#' ExistsTable("minha_tabela", .con = con)
#'
#' DBI::dbWriteTable(con, "minha_tabela", mtcars)
#'
#' # Verificar após criar
#' ExistsTable("minha_tabela", .con = con)
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
ExistsTable <- function(name, .which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbExistsTable(.con,name)
}

#' Lista as tabelas existentes no banco de dados
#'
#' @description
#' `ListTables()` retorna um vetor com os nomes de todas as tabelas
#' presentes no banco de dados. Equivale a `DBI::dbListTables()`.
#'
#' @param .which Apelido do banco de dados (opcional).
#' @param .path Caminho direto para o banco de dados (opcional).
#' @param .con Conexão DBI existente (opcional). Se fornecida, `.which` e
#'   `.path` são ignorados e a desconexão é responsabilidade do chamador.
#'
#' @returns Um vetor de strings com os nomes das tabelas.
#'
#' @examples
#' \dontrun{
#' con <- connection(.path = "banco.sqlite")
#'
#' # Criar algumas tabelas
#' DBI::dbWriteTable(con, "clientes", data.frame(id = 1:3))
#' DBI::dbWriteTable(con, "vendas", data.frame(valor = c(10, 20)))
#'
#' # Listar todas as tabelas
#' ListTables(.con = con)
#'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
ListTables <- function(.which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbListTables(.con)
}
