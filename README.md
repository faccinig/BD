# BD

Wrapper R para bancos de dados **SQLite**, com interpoção segura de SQL via `glue`.

## Instalacao

```r
# install.packages("devtools")
devtools::install_github("faccinig/BD")
```

## Uso básico

```r
library(BD)

# Conectar a um banco de dados
con <- connection(.path = "meu_banco.sqlite3")

# Criar uma tabela
Execute("CREATE TABLE pessoas (id INTEGER PRIMARY KEY, nome TEXT, idade INTEGER)", .con = con)

# Inserir dados com interpolacao segura
nome <- "Maria"
idade <- 30
Execute("INSERT INTO pessoas (nome, idade) VALUES ({nome}, {idade})", .con = con)

# Consultar dados -- retorna uma tibble
GetQuery("SELECT * FROM pessoas WHERE idade >= {idade}", .con = con)

# Fechar a conexao
DBI::dbDisconnect(con)
```

## Apelidos de bancos (path aliases)

Em vez de repetir o caminho do banco em cada chamada, registre um apelido:

```r
set_path(.path = "dados/financeiro.sqlite3", .which = "fin")
GetQuery("SELECT * FROM contas", .which = "fin")
```

Os apelidos duram apenas a sessao atual. Para apelidos permanentes, configure-os
no arquivo `config.yml`.

## As tres formas de conectar: `.which` / `.path` / `.con`

Toda funcao do pacote aceita estes tres parametros:

| Parametro  | Descricao |
|------------|-----------|
| `.which`   | Apelido registrado via `set_path()` ou `config.yml` |
| `.path`    | Caminho direto para o arquivo `.sqlite`/`.sqlite3`/`.db` |
| `.con`     | Conexao DBI existente (quem conecta, desconecta) |

## Visao geral das funcoes

### Gerenciamento de caminhos

| Funcao         | Descricao |
|----------------|-----------|
| `path()`       | Resolve o caminho de um banco a partir do apelido |
| `set_path()`   | Registra um apelido para a sessao atual |
| `clear_paths()`| Remove todos os apelidos da sessao |
| `list_paths()` | Lista apelidos da sessao e do `config.yml` |

### Conexao

| Funcao           | Descricao |
|------------------|-----------|
| `connection()`   | Cria uma conexao DBI com um banco SQLite |

### Interpolacao SQL

| Funcao       | Descricao |
|--------------|-----------|
| `glue()`     | Interpola variaveis do ambiente em SQL de forma segura |
| `glueData()` | Interpola colunas de um `data.frame` em SQL |

### Consultas

| Funcao          | Descricao |
|-----------------|-----------|
| `GetQuery()`    | Executa uma consulta e retorna uma `tibble` |
| `GetQueryData()`| Executa consultas parametrizadas por um `data.frame` |

### Execucao (INSERT, UPDATE, DELETE, DDL)

| Funcao         | Descricao |
|----------------|-----------|
| `Execute()`    | Executa uma instrucao SQL que modifica o banco |
| `ExecuteData()`| Executa instrucoes parametrizadas por um `data.frame` |

### Operacoes com tabelas

| Funcao             | Descricao |
|--------------------|-----------|
| `ReadTable()`      | Le uma tabela inteira como `data.frame` |
| `WriteTable()`     | Escreve um `data.frame` no banco (padrao: `append = TRUE`) |
| `CreateTable()`    | Cria uma tabela vazia com estrutura definida |
| `AppendTable()`    | Adiciona linhas a uma tabela existente |
| `OverwriteTable()` | Sobrescreve uma tabela completamente |
| `RemoveTable()`    | Exclui uma tabela do banco |
| `ExistsTable()`    | Verifica se uma tabela existe |
| `ListTables()`     | Lista todas as tabelas do banco |

## Configuracao (`config.yml`)

Apelidos permanentes podem ser definidos no arquivo `config.yml` na raiz do projeto:

```yaml
default:
  BD:
    default: "caminho/para/principal.sqlite3"
    financeiro: "caminho/para/financeiro.db"
```

Estes apelidos sao usados automaticamente pelo `path()` quando nenhum apelido
de sessao correspondente for encontrado. Apelidos definidos via `set_path()`
na sessao tem precedencia sobre os do `config.yml`.

## Exemplos praticos

### Insercao em lote com `ExecuteData`

```r
dados <- data.frame(
  nome = c("Ana", "Bruno", "Carla"),
  idade = c(25, 30, 28),
  stringsAsFactors = FALSE
)
ExecuteData(dados,
  "INSERT INTO pessoas (nome, idade) VALUES ({nome}, {idade})",
  .which = "fin")
```

### Consulta parametrizada com `GetQueryData`

```r
params <- data.frame(dept = c("vendas", "ti"))
GetQueryData(params,
  "SELECT nome, salario FROM funcionarios WHERE departamento = {dept}",
  .which = "rh")
```

### Ciclo completo de tabela

```r
con <- connection(.path = "app.db")

CreateTable("produtos",
  c(id = "INTEGER PRIMARY KEY", nome = "TEXT", preco = "REAL"),
  .con = con)

df <- data.frame(nome = c("Caneta", "Caderno"), preco = c(2.50, 15.00),
                 stringsAsFactors = FALSE)
WriteTable(df, "produtos", .con = con)

ReadTable("produtos", .con = con)

RemoveTable("produtos", .con = con)

DBI::dbDisconnect(con)
```

## Dependencias

- DBI, RSQLite — conexao e operacoes com SQLite
- glue — interpolacao segura de SQL
- config — apelidos de bancos via `config.yml`
- tibble — saida das consultas como tibbles
- tools — validacao de extensoes de arquivo
