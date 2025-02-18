# ETL e Web Scraping - Os Maiores Bancos do Mundo

Neste projeto prático, utilizei as habilidades adquiridas ao longo do curso ([IBM Data Engineering Professional Certificate](https://www.coursera.org/professional-certificates/ibm-data-engineer)) e criei um pipeline ETL completo para acessar dados de um site e processá-los para atender aos requisitos.

## Cenário do Projeto

Você foi contratado como engenheiro de dados por uma organização de pesquisa. Seu chefe pediu que você criasse um código que possa ser usado para compilar a lista dos 10 maiores bancos do mundo classificados por capitalização de mercado em bilhões de USD. Além disso, os dados precisam ser transformados e armazenados em GBP, EUR e INR também, de acordo com as informações da taxa de câmbio que foram disponibilizadas para você como um arquivo CSV. A tabela de informações processadas deve ser salva localmente em formato CSV e como uma tabela de banco de dados.

Seu trabalho é criar um sistema automatizado para gerar essas informações para que o mesmo possa ser executado em cada trimestre financeiro para preparar o relatório.

## Tarefas do Projeto

**Tarefa 1:**
- Escreva uma função log_progress() para registrar o progresso do código em diferentes estágios em um arquivo code_log.txt. Use a lista de pontos de log fornecida para criar entradas de log em cada estágio do código.

**Tarefa 2:**
- Extraia as informações tabulares da URL fornecida sob o título ‘Por capitalização de mercado’ e salve em um dataframe.
  - Inspecione a página da web e identifique a posição e o padrão das informações tabulares no código HTML
  - Escreva o código para uma função extract() para realizar a extração de dados necessária.
  - Execute uma chamada de função para extract() para verificar a saída.

**Tarefa 3**
Transforme o dataframe adicionando colunas para a Capitalização de Mercado em GBP, EUR e INR, arredondadas para 2 casas decimais, com base nas informações da taxa de câmbio compartilhadas como um arquivo CSV.
  - Escreva o código para uma função transform() para realizar a tarefa mencionada.
  - Execute uma chamada de função para transform() e verifique a saída.

**Tarefa 4:**
  - Carregue o dataframe transformado em um arquivo CSV de saída. Escreva uma função load_to_csv(), execute uma chamada de função e verifique a saída.

**Tarefa 5:**
  - Carregue o dataframe transformado em um servidor de banco de dados SQL como uma tabela. Escreva uma função load_to_db(), execute uma chamada de função e verifique a saída.

**Tarefa 6:**
  - Execute consultas na tabela do banco de dados. Escreva uma função load_to_db(), execute um conjunto de consultas fornecidas e verifique a saída.

**Tarefa 7:**
  - Verifique se as entradas de log foram completadas em todas as etapas, verificando o conteúdo do arquivo code_log.txt.

## Bibliotecas Utilizadas

1 - ```requests``` - A biblioteca usada para acessar as informações a partir da URL.

2 - ```bs4``` - A biblioteca que contém a função ```BeautifulSoup``` usada para webscraping.

3 - ```pandas``` - A biblioteca usada para processar os dados extraídos, armazená-los nos formatos necessários e comunicar-se com os bancos de dados.

4 - ```sqlite3``` - A biblioteca necessária para criar uma conexão com o servidor de banco de dados.

5 - ```numpy``` - A biblioteca necessária para a operação de arredondamento matemático conforme exigido nos objetivos.

6 - ```datetime``` - A biblioteca que contém a função ```datetime``` usada para extrair o timestamp para fins de registro.


## Estrutura do código

```python
def log_progress(message):
''' Esta função registra a mensagem mencionada de um determinado estágio da
execução do código em um arquivo de log. A função não retorna nada'''

def extract(url, table_attribs):
''' Esta função tem como objetivo extrair as
informações necessárias do site e salvá-las em um data frame. A
função retorna o data frame para processamento posterior. '''
  return df

def transform(df, csv_path):
''' Esta função acessa o arquivo CSV para obter informações sobre a
taxa de câmbio e adiciona três colunas ao data frame, cada uma
contendo a versão transformada da coluna Market Cap para
respectivas moedas'''
  return df

def load_to_csv(df, output_path):
''' Esta função salva o data frame final como um arquivo CSV no
caminho fornecido. A função não retorna nada.'''

def load_to_db(df, sql_connection, table_name):
''' Esta função salva o quadro de dados final em uma tabela
de banco de dados com o nome fornecido. A função não retorna nada.'''

def run_query(query_statement, sql_connection):
''' Esta função executa a consulta na tabela de banco de dados e
imprime a saída no terminal. A função não retorna nada. '''
```

## Conclusão

Agora sou capaz de:

- Usar técnicas de Webscraping para extrair informações de qualquer site conforme necessário.

- Usar data frames e dicionários do Pandas para transformar dados conforme necessário.

- Carregar as informações processadas em arquivos CSV e como tabelas de banco de dados.

- Consultar as tabelas do banco de dados usando as bibliotecas SQLite3 e pandas.

- Registrar o progresso do código adequadamente.
