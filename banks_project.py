# Importando as bibliotecas necessárias
import requests
import pandas as pd 
import sqlite3
import numpy as np 
from datetime import datetime
from bs4 import BeautifulSoup

def log_progress(message):
    """Esta função registra a mensagem mencionada de um determinado estágio da
    execução do código em um arquivo de log. A função não retorna nada
    """
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    
    now = datetime.now()

    timestamp = now.strftime(timestamp_format)

    with open("./logs/code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    """Esta função tem como objetivo extrair as informações
    necessárias do site e salvá-las em um data frame. A função
    retorna o data frame para processamento posterior.
    """
    response = requests.get(url)
    response.raise_for_status()  # Garante que a requisição foi bem-sucedida

    soup = BeautifulSoup(response.text, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})

    table = tables[0]  

    rows = table.find_all("tr")[1:]  # Ignorar o cabeçalho

    data = []
    for row in rows:
        cols = row.find_all("td")
        if len(cols) >= 3:
            bank_name = cols[1].text.strip()  # Nome do banco
            total_assets = cols[2].text.strip().replace(",", "")  # Remover vírgulas dos números
            try:
                total_assets = float(total_assets)  # Converter para float
            except ValueError:
                total_assets = None  # Caso não seja um número válido

            data.append([bank_name, total_assets])
            df = pd.DataFrame(data, columns=table_attribs)

    log_progress("Processo de extração concluído")

    return df  

def transform(df, csv_path):
    """Esta função acessa o arquivo CSV para obter informações sobre a taxa de câmbio
    e adiciona três colunas ao quadro de dados, cada
    contendo a versão transformada da coluna Market Cap para
    respectivas moedas
    """
    try:
        exchange_rate = pd.read_csv(csv_path)
        exchange_rate = exchange_rate.set_index("Currency").to_dict()["Rate"]
    except Exception as e:
        log_progress(f"Erro ao ler CSV: {e}")
        return df  # Retorna o dataframe sem alterações

    # Aplicar a conversão apenas se os valores forem numéricos
    if 'MC_USD_Billion' in df.columns:
        for currency in ['GBP', 'EUR', 'INR']:
            df[f"MC_{currency}_Billion"] = df["MC_USD_Billion"].apply(
                lambda x: np.round(x * exchange_rate.get(currency, 1), 2) if pd.notna(x) else None
            )

    log_progress("Processo de transformação concluída")

    return df

def load_to_csv(df, output_path):
    """Esta função salva o quadro de dados final como um arquivo CSV no
    caminho fornecido. A função não retorna nada.
    """
    log_progress("Dados salvos no arquivo CSV")

    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    """Esta função salva o quadro de dados final em uma tabela
    de banco de dados com o nome fornecido. A função não retorna nada.
    """
    log_progress("Dados carregados no Banco de Dados como uma tabela.")

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statatement, sql_connection):
    """Esta função executa a consulta na tabela do banco de dados e
    imprime a saída no terminal. A função não retorna nada.
    """
    log_progress("Executando consulta")

    print(query_statatement)
    query_output = pd.read_sql(query_statatement, sql_connection)

    print(query_output)

# Definindo as entidades necessárias
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ['Name', 'MC_USD_Billion']
csv_path = "./data/exchange_rate.csv"
output_path = './data/Largest_banks_data.csv'
db_name = "./data/Banks.db"
table_name = "Largest_banks"

log_progress("Preliminares concluídas. Iniciando o processo ETL")
df = extract(url, table_attribs)

log_progress("Iniciando o processo de transformação")
df = transform(df, csv_path)

log_progress("Iniciando o processo de Carga")
load_to_csv(df, output_path)

log_progress("Conexão SQL iniciada")
sql_connection = sqlite3.connect(db_name)
load_to_db(df, sql_connection, table_name)

log_progress("Primeira consulta:")
query = f'SELECT * FROM {table_name};'
run_query(query, sql_connection)

log_progress("Segunda consulta:")
query = f'SELECT AVG(MC_GBP_Billion) FROM {table_name};'
run_query(query, sql_connection)

log_progress("Terceira consulta:")
query = f'SELECT name FROM {table_name} LIMIT 5;'
run_query(query, sql_connection)

log_progress("Processo completo")
sql_connection.close()
