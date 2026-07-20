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
