BD_env <- new.env(parent = emptyenv())

get_paths <- function() {
  get("paths", envir = BD_env, inherits = FALSE)
}

set_paths <- function(paths) {
  assign("paths", paths, envir = BD_env, inherits = FALSE)
}

set_paths(list())
