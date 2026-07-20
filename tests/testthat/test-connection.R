context("Database connections")

test_that("connection() creates a valid SQLite connection with .path", {
  tmp <- tempfile(fileext = ".sqlite3")
  on.exit(unlink(tmp))

  con <- connection(.path = tmp)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  expect_s4_class(con, "SQLiteConnection")
  expect_true(DBI::dbIsValid(con))
})

test_that("connection() creates a valid SQLite connection with .which", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))

  clear_paths()
  on.exit(clear_paths(), add = TRUE)

  set_path(.path = tmp, .which = "conn_test_db")
  con <- connection(.which = "conn_test_db")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  expect_s4_class(con, "SQLiteConnection")
  expect_true(DBI::dbIsValid(con))
})

test_that("connection() errors on invalid path", {
  expect_error(connection(.path = "nonexistent.xyz"), "doesn't have a valid extension!")
})

test_that("connection() supports .db extension", {
  tmp <- tempfile(fileext = ".db")
  on.exit(unlink(tmp))

  con <- connection(.path = tmp)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  expect_s4_class(con, "SQLiteConnection")
  expect_true(DBI::dbIsValid(con))
})

test_that("connection() allows DDL and DML operations", {
  tmp <- tempfile(fileext = ".sqlite")
  on.exit(unlink(tmp))

  con <- connection(.path = tmp)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE test (id INTEGER, name TEXT)")
  DBI::dbExecute(con, "INSERT INTO test (id, name) VALUES (1, 'hello')")
  res <- DBI::dbGetQuery(con, "SELECT * FROM test")
  expect_equal(nrow(res), 1L)
  expect_equal(res$id, 1L)
  expect_equal(res$name, "hello")
})

test_that("connection() errors on unresolvable .which", {
  clear_paths()
  setup_config_env()
  on.exit(teardown_config_env(), add = TRUE)
  expect_error(connection(.which = "nonexistent_alias_xyz"), "invalid path!")
})
