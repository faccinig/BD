config_env <- new.env(parent = emptyenv())

setup_config_env <- function() {
  if (!is.null(config_env$tmp)) {
    teardown_config_env()
  }
  tmp <- tempfile("bd_test_config_")
  dir.create(tmp, showWarnings = FALSE)
  writeLines(
    'default:\n  BD:\n    default: "tests/bases_dados/lite.sqlite3"\n    lite: "tests/bases_dados/lite.sqlite3"',
    file.path(tmp, "config.yml")
  )
  old_wd <- getwd()
  setwd(tmp)

  config_env$tmp <- tmp
  config_env$old_wd <- old_wd
}

teardown_config_env <- function() {
  if (!is.null(config_env$old_wd)) {
    setwd(config_env$old_wd)
  }
  if (!is.null(config_env$tmp)) {
    unlink(config_env$tmp, recursive = TRUE, force = TRUE)
  }
  config_env$old_wd <- NULL
  config_env$tmp <- NULL
}
