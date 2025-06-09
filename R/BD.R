# Path -------------------------------------------------------------------------

#' Identifica o banco de dados usado
#'
#' `path()` identifica qual banco de dados está sendo usado pelo usuário
#'
#' @param .which Um character com o "apelido" do banco de dados
#' (geralmente usado em conjunto com um arquivo config.yml).
#' @param .path Endereço do banco de dados (opcional).
#'
#' @returns Um character com o caminho do banco de dados associado ao apelido.
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


#' Define o endereço do banco de dados usado
#'
#' `set_path()` define o endereço do banco de dados a ser utilizado
#'
#' @param .path Endereço do banco de dados.
#' @param .which Um character com o "apelido" do banco de dados
#' (geralmente usado em conjunto com um arquivo config.yml).
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
            "`.which` must have length 1!" = length(.path) == 1L)

  paths <- get_paths()
  paths[[.which]] <- .path
  set_paths(paths)

  invisible()
}

#' Remove endereço do banco de dados salvos
#'
#' `clear_paths()` deleta endereços de bancos de dados que porventura estejam salvos na sessão.
#'
#' @export
clear_paths <- function() {
  set_paths(list())
  invisible()
}

#' Lista os bancos de dados salvos
#'
#' `list_paths()`lista endereços de bancos de dados que porventura estejam salvos na sessão.
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


#' Relaciona dados da conexão com banco de dados
#'
#' `connection()` mostra informações da conexão ativa com o banco de dados.
#'
#' @param .path Endereço do banco de dados.
#' @param .which Um character com o "apelido" do banco de dados
#' (geralmente usado em conjunto com um arquivo config.yml).
#'
#' @return Um character com os dados do banco de dados.
#'
#' @export
connection <- function(.which = NULL, .path = NULL) {
  .path <- path(.which, .path)
  if (is.null(.path)) stop("N")
  type <- BD_type(.path)
  if (is.null(type)) stop("invalid path!")
  switch (type,
    access = DBI::dbConnect(odbc::odbc(),
                            Driver = "{Microsoft Access Driver (*.mdb, *.accdb)}",
                            Mode = "Share Deny None",
                            Dbq = .path,
                            encoding = "Latin1"),
    lite = DBI::dbConnect(RSQLite::SQLite(), .path, extended_types = TRUE)
  )
}


# db ----

#' @export
glueData <- function(.data, stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  glue::glue_data_sql(.data, stmt, .sep = "\n", .con = .con, .envir = .envir)
}

#' @export
glue <- function(stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  glue::glue_sql(stmt, .sep = "\n", .con = .con, .envir = .envir)
}

#' @export
GetQuery <- function(stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  sql <- glue(stmt, .con = .con, .envir = .envir)

  tibble::as_tibble(DBI::dbGetQuery(.con, sql))
}

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

#' @export
ReadTable <- function(name, .which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbReadTable(.con, name)
}

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

#' @export
CreateTable <- function(name, fields,
                        .which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbCreateTable(.con, name = name, fields = fields)
}

#' @export
AppendTable <- function(value, name, .which = NULL, .path = NULL, .con = NULL) {
  WriteTable(value, name, append = TRUE,.which = .which, .path = .path, .con = .con)
}

#' @export
OverwriteTable <- function(value, name,
                           .which = NULL, .path = NULL, .con = NULL,
                           fields = NULL) {
  WriteTable(value, name, append = FALSE,
             .which = .which, .path = .path, .con = .con,
             fields = fields)
}

#' @export
RemoveTable <- function(name, .which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbRemoveTable(.con,name)
}

#' @export
ExistsTable <- function(name, .which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbExistsTable(.con,name)
}

#' @export
ListTables <- function(.which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbListTables(.con)
}


