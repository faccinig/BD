context("Database operations")

setup_temp_db <- function() {
  tmp <- tempfile(fileext = ".sqlite")
  con <- DBI::dbConnect(RSQLite::SQLite(), tmp, extended_types = TRUE)
  DBI::dbExecute(con, "CREATE TABLE mtcars (mpg REAL, cyl INTEGER, disp REAL,
    hp INTEGER, drat REAL, wt REAL, qsec REAL, vs INTEGER, am INTEGER,
    gear INTEGER, carb INTEGER)")
  DBI::dbWriteTable(con, "mtcars", mtcars, append = TRUE)
  list(con = con, path = tmp)
}

teardown_temp_db <- function(s) {
  DBI::dbDisconnect(s$con)
  unlink(s$path)
}

test_that("glue() produces SQL-safe strings", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  result <- glue("SELECT * FROM mtcars WHERE cyl = {4}", .con = s$con)
  expect_s4_class(result, "SQL")
  expect_match(as.character(result), "SELECT")
  expect_match(as.character(result), "4")
})

test_that("glue() creates a connection if not provided", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  result <- glue("SELECT * FROM mtcars WHERE gear = {4}", .path = s$path)
  expect_s4_class(result, "SQL")
})

test_that("glueData() interpolates .data columns", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  data <- data.frame(cyl_val = 4, stringsAsFactors = FALSE)
  result <- glueData(data, "SELECT * FROM mtcars WHERE cyl = {cyl_val}", .con = s$con)
  expect_s4_class(result, "SQL")
})

test_that("GetQuery() returns query results", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  res <- GetQuery("SELECT * FROM mtcars WHERE cyl = {4}", .con = s$con)
  expect_s3_class(res, "tbl_df")
  expect_equal(nrow(res), 11L)
  expect_true(all(res$cyl == 4))
})

test_that("GetQuery() creates connection from .path", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  res <- GetQuery("SELECT * FROM mtcars WHERE am = {1}", .path = s$path)
  expect_s3_class(res, "tbl_df")
  expect_equal(nrow(res), 13L)
})

test_that("GetQuery() works without curly-brace interpolation", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  res <- GetQuery("SELECT count(*) AS n FROM mtcars", .con = s$con)
  expect_equal(res$n, 32L)
})

test_that("GetQueryData() with single-element .data returns one result", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  data <- data.frame(cyl_val = 6, stringsAsFactors = FALSE)
  res <- GetQueryData(data, "SELECT * FROM mtcars WHERE cyl = {cyl_val}", .con = s$con)
  expect_s3_class(res, "tbl_df")
  expect_equal(nrow(res), 7L)
  expect_true(all(res$cyl == 6))
})

test_that("GetQueryData() with multi-row .data combines results", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  data <- data.frame(cyl_val = c(4, 6), stringsAsFactors = FALSE)
  res <- GetQueryData(data, "SELECT * FROM mtcars WHERE cyl = {cyl_val}", .con = s$con)
  expect_s3_class(res, "tbl_df")
  expect_equal(nrow(res), 11L + 7L)
  expect_true(all(res$cyl %in% c(4, 6)))
})

test_that("Execute() runs DML and returns rows affected", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  res <- Execute("INSERT INTO mtcars (mpg, cyl) VALUES (99, 8)", .con = s$con)
  expect_equal(res, 1L)

  rows <- DBI::dbGetQuery(s$con, "SELECT count(*) AS n FROM mtcars WHERE mpg = 99")
  expect_equal(rows$n, 1L)
})

test_that("Execute() handles SQL with multiple curly-brace substitutions", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  cyl_val <- 8
  gear_val <- 3
  res <- Execute(
    "UPDATE mtcars SET mpg = 99 WHERE cyl = {cyl_val} AND gear = {gear_val}",
    .con = s$con)
  expect_gt(res, 0L)

  rows <- DBI::dbGetQuery(s$con, "SELECT count(*) AS n FROM mtcars WHERE mpg = 99")
  expect_gt(rows$n, 0L)
})

test_that("ExecuteData() runs DML with .data interpolation", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  data <- data.frame(m = c(11, 22), c = c(3, 3), stringsAsFactors = FALSE)
  res <- ExecuteData(data,
    "INSERT INTO mtcars (mpg, cyl) VALUES ({m}, {c})", .con = s$con)
  expect_equal(res, 2L)

  rows <- DBI::dbGetQuery(s$con, "SELECT count(*) AS n FROM mtcars WHERE mpg IN (11, 22)")
  expect_equal(rows$n, 2L)
})

test_that("Execute() with .con bypasses connection creation", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  res <- Execute("CREATE TABLE IF NOT EXISTS bypass_test (x INTEGER)", .con = s$con)
  tables <- DBI::dbListTables(s$con)
  expect_true("bypass_test" %in% tables)
})

test_that("GetQuery() returns empty tibble for empty result", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  res <- GetQuery("SELECT * FROM mtcars WHERE cyl = {-1}", .con = s$con)
  expect_equal(nrow(res), 0L)
})

test_that("glueData() auto-connects via .path", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  data <- data.frame(cyl_val = 4, stringsAsFactors = FALSE)
  result <- glueData(data, "SELECT * FROM mtcars WHERE cyl = {cyl_val}", .path = s$path)
  expect_s4_class(result, "SQL")
})

test_that("GetQueryData() auto-connects via .path", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  data <- data.frame(cyl_val = 6, stringsAsFactors = FALSE)
  res <- GetQueryData(data, "SELECT * FROM mtcars WHERE cyl = {cyl_val}", .path = s$path)
  expect_s3_class(res, "tbl_df")
  expect_equal(nrow(res), 7L)
})

test_that("ExecuteData() auto-connects via .path", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  data <- data.frame(m = c(33, 44), c = c(3, 3), stringsAsFactors = FALSE)
  res <- ExecuteData(data,
    "INSERT INTO mtcars (mpg, cyl) VALUES ({m}, {c})", .path = s$path)
  expect_equal(res, 2L)
})

test_that("Execute() auto-connects via .path", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  res <- Execute(
    "INSERT INTO mtcars (mpg, cyl) VALUES (77, 8)", .path = s$path)
  expect_equal(res, 1L)

  rows <- DBI::dbGetQuery(s$con, "SELECT count(*) AS n FROM mtcars WHERE mpg = 77")
  expect_equal(rows$n, 1L)
})
