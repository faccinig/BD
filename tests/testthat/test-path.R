context("Path management")

test_that("path() returns supplied .path when valid extension", {
  expect_equal(path(.path = "test.sqlite"), "test.sqlite")
  expect_equal(path(.path = "test.sqlite3"), "test.sqlite3")
  expect_equal(path(.path = "test.db"), "test.db")
})

test_that("path() errors on invalid .path input", {
  expect_error(path(.path = 123), "`.path` must be a character!")
  expect_error(path(.path = c("a", "b")), "`.path` must have length 1!")
  expect_error(path(.path = "test.txt"), "doesn't have a valid extension!")
  expect_error(path(.path = "test.accdb"), "doesn't have a valid extension!")
})

test_that("path() resolves session-stored alias via set_path()", {
  clear_paths()
  set_path(.path = "my.sqlite", .which = "mydb")
  expect_equal(path(.which = "mydb"), "my.sqlite")
})

test_that("path() returns NULL for unknown alias when no session entry exists", {
  clear_paths()
  setup_config_env()
  on.exit(teardown_config_env(), add = TRUE)
  expect_null(path(.which = "nonexistent_alias_xyz"))
})

test_that("path() defaults to .which = \"default\" with config fallback", {
  clear_paths()
  setup_config_env()
  on.exit(teardown_config_env(), add = TRUE)
  result <- path()
  expect_type(result, "character")
})

test_that("path() errors on invalid .which input", {
  expect_error(path(.which = 123), "`.which` must be a character!")
  expect_error(path(.which = c("a", "b")), "`.which` must have length 1!")
})

test_that("set_path() stores paths in session", {
  clear_paths()
  set_path(.path = "session.sqlite", .which = "test_alias")
  paths <- BD:::get_paths()
  expect_equal(paths[["test_alias"]], "session.sqlite")
})

test_that("set_path() overwrites existing alias", {
  clear_paths()
  set_path(.path = "first.sqlite", .which = "alias")
  set_path(.path = "second.sqlite", .which = "alias")
  expect_equal(path(.which = "alias"), "second.sqlite")
})

test_that("set_path() rejects invalid extensions", {
  expect_error(set_path(.path = "data.csv", .which = "x"),
               "not suported!")
})

test_that("set_path() validates inputs", {
  expect_error(set_path(.path = NULL), "`.path` must be defined!")
  expect_error(set_path(.path = 123), "`.path` must be a character!")
  expect_error(set_path(.path = c("a", "b")), "`.path` must have length 1!")
  expect_error(set_path(.path = "x.sqlite", .which = c("a", "b")), "`.which` must have length 1!")
})

test_that("clear_paths() removes all session paths", {
  set_path(.path = "a.sqlite", .which = "alias1")
  set_path(.path = "b.sqlite", .which = "alias2")
  clear_paths()
  paths <- BD:::get_paths()
  expect_equal(length(paths), 0L)
})

test_that("list_paths() returns session paths merged with config", {
  clear_paths()
  setup_config_env()
  on.exit(teardown_config_env(), add = TRUE)

  lst <- list_paths()
  expect_type(lst, "list")

  set_path(.path = "zzz.sqlite", .which = "unique_test_alias")
  lst <- list_paths()
  expect_equal(lst[["unique_test_alias"]], "zzz.sqlite")
})

test_that("get_paths() and set_paths() manipulate BD_env correctly", {
  BD:::set_paths(list(a = "x.sqlite", b = "y.sqlite"))
  p <- BD:::get_paths()
  expect_equal(p[["a"]], "x.sqlite")
  expect_equal(p[["b"]], "y.sqlite")
  BD:::set_paths(list())
  expect_equal(length(BD:::get_paths()), 0L)
})

test_that("list_paths() handles missing config.yml gracefully", {
  clear_paths()
  tmp <- tempfile("bd_no_config_")
  dir.create(tmp, showWarnings = FALSE)
  old_wd <- getwd()
  on.exit({ setwd(old_wd); unlink(tmp, recursive = TRUE, force = TRUE) })

  setwd(tmp)
  lst <- list_paths()
  expect_type(lst, "list")
  expect_equal(length(lst), 0L)
})
