# Path -------------------------------------------------------------------------

#' Identifica o banco de dados usado
#'
#' `BD_path()` identifica qual banco de dados está sendo usado pelo usuário
#'
#' @param .which Um character com o "apelido" do banco de dados
#' (geralmente usado em conjunto com um arquivo config.yml).
#' @param .path Endereço do banco de dados (opcional).
#'
#' @returns Um character com o caminho do banco de dados associado ao apelido.
#'
#' @export
BD_path <- function(.which = NULL, .path = NULL) {
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
#' `BD_set_path()` define o endereço do banco de dados a ser utilizado
#'
#' @param .path Endereço do banco de dados.
#' @param .which Um character com o "apelido" do banco de dados
#' (geralmente usado em conjunto com um arquivo config.yml).
#'
#' @export
BD_set_path <- function(.path = NULL, .which = "default") {

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
#' `BD_clear_paths()` deleta endereços de bancos de dados que porventura estejam salvos na sessão.
#'
#' @export
BD_clear_paths <- function() {
  set_paths(list())
  invisible()
}

#' Lista os bancos de dados salvos
#'
#' `BD_list_paths()`lista endereços de bancos de dados que porventura estejam salvos na sessão.
#'
#' @export
BD_list_paths <- function() {
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
          "accdb" = "access",
          "mdb" = "access",
          "sqlite" = "lite",
          "sqlite3" = "lite",
          "db" = "lite")
}

# Connection -------------------------------------------------------------------


#' Relaciona dados da conexão com banco de dados
#'
#' `BD_connection()` mostra informações da conexão ativa com o banco de dados.
#'
#' @param .path Endereço do banco de dados.
#' @param .which Um character com o "apelido" do banco de dados
#' (geralmente usado em conjunto com um arquivo config.yml).
#'
#' @return Um character com os dados do banco de dados.
#'
#' @export
BD_connection <- function(.which = NULL, .path = NULL) {
  .path <- BD_path(.which, .path)
  if (is.null(.path)) stop("N")
  type <- BD_type(.path)
  if (is.null(type)) stop("invalid path!")
  switch (type,
    access = DBI::dbConnect(odbc::odbc(),
                            Driver = "{Microsoft Access Driver (*.mdb, *.accdb)}",
                            Mode = "Share Deny None",
                            Dbq = .path,
                            encoding = "Latin1"),
    lite = DBI::dbConnect(RSQLite::SQLite(), .path)
  )
}


# db ----

#' @export
BD_glueData <- function(.data, stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- BD_connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  glue::glue_data_sql(.data, stmt, .sep = "\n", .con = .con, .envir = .envir)
}

#' @export
BD_glue <- function(stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- BD_connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  glue::glue_sql(stmt, .sep = "\n", .con = .con, .envir = .envir)
}

#' @export
BD_GetQuery <- function(stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- BD_connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  sql <- BD_glue(stmt, .con = .con, .envir = .envir)

  tibble::as_tibble(DBI::dbGetQuery(.con, sql))
}

#' @export
BD_GetQueryData <- function(.data, stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- BD_connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  sql <- BD_glueData(.data, stmt, .con = .con, .envir = .envir)
  if (length(sql) == 1L) return(BD_GetQuery(sql, .con = .con))

  tbls <- lapply(sql, BD_GetQuery, .con = .con)
  tbls <- do.call(rbind, tbls)

  tibble::as_tibble(tbls)
}

#' @export
BD_ExecuteData <- function(.data, stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- BD_connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  sql <- BD_glueData(.data, stmt, .con = .con, .envir = .envir)
  res <- vapply(sql, function(statement) DBI::dbExecute(.con, statement), integer(1L))

  sum(res)
}

#' @export
BD_Execute <- function(stmt, .which = NULL, .path = NULL, .con = NULL, .envir = parent.frame()) {
  if (is.null(.con)) {
    .con <- BD_connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  sql <- BD_glue(stmt, .con = .con, .envir = .envir)
  res <- vapply(sql, function(statement) DBI::dbExecute(.con, statement), integer(1L))

  sum(res)
}

#' @export
BD_ReadTable <- function(name, .which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- BD_connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbReadTable(.con, name)
}

#' @export
BD_WriteTable <- function(value, name, append = TRUE, .which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- BD_connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  overwrite <- !append
  # as funções abaixo estão sendo utilizadas de forma inadequada. o ideal era
  # criar o método S4 para "ACCESS" e "SQLiteConnection" mas ao substituir o
  # método para "SQLiteConnection" original - do RSQlite - não sei como retornar
  # para que o método original dê sequencia..
  # talvez a solução seja criar um novo tipo de objeto que herde do "SQLiteConnection"
  WriteTable <- switch (class(.con),
                        "ACCESS" = WriteTable_access,
                        "SQLiteConnection" = WriteTable_lite)
  WriteTable(.con, value, name, append, overwrite)
}


WriteTable_access <- function(.con, value, name, append, overwrite) {
  DBI::dbWriteTable(.con, name, value,
                    append = append ,
                    overwrite = overwrite,
                    batch_rows = 1L)
}

WriteTable_lite <- function(.con, value, name, append, overwrite) {

  dt_col <- vapply(value,
                   function(x) inherits(x, "Date") | inherits(x, "POSIXt"),
                   logical(1))

  value[dt_col] <- lapply(value[dt_col], as.character)

  dt_col <- vapply(value,
                   function(x) inherits(x, "Period"),
                   logical(1))

  value[dt_col] <- lapply(value[dt_col], function(x) format(lubridate::as_date(.x),"%T"))

  DBI::dbWriteTable(.con, name, value,
                    append = append ,
                    overwrite = overwrite)
}



#' @export
BD_AppendTable <- function(value, name, .which = NULL, .path = NULL, .con = NULL) {
  BD_WriteTable(value, name, append = TRUE,.which = .which, .path = .path, .con = .con)
}

#' @export
BD_RemoveTable <- function(name, .which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- BD_connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbRemoveTable(.con,name)
}

#' @export
BD_ExistsTable <- function(name, .which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- BD_connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbExistsTable(.con,name)
}

#' @export
BD_ListTables <- function(.which = NULL, .path = NULL, .con = NULL) {
  if (is.null(.con)) {
    .con <- BD_connection(.which = .which, .path = .path)
    on.exit(DBI::dbDisconnect(.con))
  }

  DBI::dbListTables(.con)
}


