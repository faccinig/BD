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
