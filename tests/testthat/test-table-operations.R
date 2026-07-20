context("Table operations")

setup_temp_db <- function() {
  tmp <- tempfile(fileext = ".sqlite")
  con <- DBI::dbConnect(RSQLite::SQLite(), tmp, extended_types = TRUE)
  list(con = con, path = tmp)
}

teardown_temp_db <- function(s) {
  DBI::dbDisconnect(s$con)
  unlink(s$path)
}

test_that("WriteTable() writes data to the database", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  WriteTable(mtcars, "mtcars_table", .con = s$con)
  expect_true(DBI::dbExistsTable(s$con, "mtcars_table"))
  expect_equivalent(DBI::dbReadTable(s$con, "mtcars_table"), mtcars)
})

test_that("WriteTable() with append = FALSE overwrites", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  small <- mtcars[1:5, ]
  WriteTable(small, "overwrite_test", .con = s$con)
  WriteTable(small, "overwrite_test", .con = s$con, append = FALSE)
  expect_equal(nrow(DBI::dbReadTable(s$con, "overwrite_test")), 5L)
})

test_that("WriteTable() with append = TRUE appends rows", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  part1 <- mtcars[1:5, ]
  part2 <- mtcars[6:10, ]

  WriteTable(part1, "append_test", .con = s$con)
  WriteTable(part2, "append_test", .con = s$con, append = TRUE)

  res <- DBI::dbReadTable(s$con, "append_test")
  expect_equal(nrow(res), 10L)
})

test_that("ReadTable() reads a full table", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  DBI::dbWriteTable(s$con, "read_test", iris, overwrite = TRUE)
  res <- ReadTable("read_test", .con = s$con)
  expect_equal(names(res), names(iris))
  expect_equal(nrow(res), nrow(iris))
  expect_equal(as.character(res$Species), as.character(iris$Species))
  expect_equal(res$Sepal.Length, iris$Sepal.Length, tolerance = 1e-8)
})

test_that("CreateTable() creates an empty table", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  fields <- c(id = "INTEGER", name = "TEXT", value = "REAL")
  CreateTable("created_table", fields, .con = s$con)

  expect_true(DBI::dbExistsTable(s$con, "created_table"))
  res <- DBI::dbReadTable(s$con, "created_table")
  expect_equal(nrow(res), 0L)
  expect_true(all(c("id", "name", "value") %in% names(res)))
})

test_that("AppendTable() is equivalent to WriteTable() with append = TRUE", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  WriteTable(mtcars[1:3, ], "append_via_wrapper", .con = s$con)
  AppendTable(mtcars[4:6, ], "append_via_wrapper", .con = s$con)

  res <- DBI::dbReadTable(s$con, "append_via_wrapper")
  expect_equal(nrow(res), 6L)
})

test_that("OverwriteTable() replaces all data", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  WriteTable(mtcars, "overwrite_via_wrapper", .con = s$con)
  OverwriteTable(iris, "overwrite_via_wrapper", .con = s$con)

  expect_equal(nrow(DBI::dbReadTable(s$con, "overwrite_via_wrapper")), 150L)
})

test_that("ExistsTable() correctly reports table existence", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  expect_false(ExistsTable("phantom_table", .con = s$con))
  DBI::dbWriteTable(s$con, "real_table", mtcars)
  expect_true(ExistsTable("real_table", .con = s$con))
})

test_that("RemoveTable() deletes a table", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  DBI::dbWriteTable(s$con, "to_remove", mtcars)
  expect_true(DBI::dbExistsTable(s$con, "to_remove"))

  RemoveTable("to_remove", .con = s$con)
  expect_false(DBI::dbExistsTable(s$con, "to_remove"))
})

test_that("ListTables() returns names of all tables", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  DBI::dbWriteTable(s$con, "table_a", mtcars[1:2, ])
  DBI::dbWriteTable(s$con, "table_b", mtcars[1:2, ])

  tables <- ListTables(.con = s$con)
  expect_true("table_a" %in% tables)
  expect_true("table_b" %in% tables)
})

test_that("Table CRUD functions work with .path instead of .con", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  WriteTable(iris, "path_write_test", .path = s$path)
  res <- ReadTable("path_write_test", .path = s$path)
  expect_equal(nrow(res), nrow(iris))
  expect_equal(as.character(res$Species), as.character(iris$Species))
  expect_equal(res$Sepal.Length, iris$Sepal.Length, tolerance = 1e-8)

  RemoveTable("path_write_test", .path = s$path)
  expect_false(DBI::dbExistsTable(s$con, "path_write_test"))
})

test_that("WriteTable() accepts field.types parameter", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  df <- data.frame(x = 1L, stringsAsFactors = FALSE)
  WriteTable(df, "typed_table", .con = s$con,
             append = FALSE, fields = c(x = "TEXT"))
  res <- DBI::dbGetQuery(s$con, "SELECT typeof(x) AS x_type FROM typed_table")
  expect_equal(res$x_type, "text")
})

test_that("Full table lifecycle: Create -> Write -> Read -> Remove", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  fields <- c(id = "INTEGER PRIMARY KEY", label = "TEXT")
  CreateTable("lifecycle", fields, .con = s$con)
  expect_true(ExistsTable("lifecycle", .con = s$con))
  expect_equal(ListTables(.con = s$con), "lifecycle")

  df <- data.frame(id = 1:3, label = c("a", "b", "c"), stringsAsFactors = FALSE)
  WriteTable(df, "lifecycle", .con = s$con, append = FALSE)

  res <- ReadTable("lifecycle", .con = s$con)
  expect_equal(nrow(res), 3L)
  expect_equal(res$id, 1:3)

  RemoveTable("lifecycle", .con = s$con)
  expect_false(ExistsTable("lifecycle", .con = s$con))
})

test_that("CreateTable() auto-connects via .path", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  CreateTable("auto_create", c(id = "INTEGER", label = "TEXT"), .path = s$path)
  expect_true(DBI::dbExistsTable(s$con, "auto_create"))
})

test_that("ExistsTable() auto-connects via .path", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  DBI::dbWriteTable(s$con, "auto_exists", mtcars[1:2, ])
  expect_true(ExistsTable("auto_exists", .path = s$path))
})

test_that("ListTables() auto-connects via .path", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  DBI::dbWriteTable(s$con, "auto_list_a", mtcars[1:2, ])
  tables <- ListTables(.path = s$path)
  expect_true("auto_list_a" %in% tables)
})

test_that("AppendTable() auto-connects via .path", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  WriteTable(mtcars[1:3, ], "auto_append", .con = s$con)
  AppendTable(mtcars[4:6, ], "auto_append", .path = s$path)
  expect_equal(nrow(DBI::dbReadTable(s$con, "auto_append")), 6L)
})

test_that("OverwriteTable() auto-connects via .path", {
  s <- setup_temp_db()
  on.exit(teardown_temp_db(s))

  WriteTable(mtcars, "auto_overwrite", .con = s$con)
  OverwriteTable(mtcars[1:5, ], "auto_overwrite", .path = s$path)
  expect_equal(nrow(DBI::dbReadTable(s$con, "auto_overwrite")), 5L)
})
