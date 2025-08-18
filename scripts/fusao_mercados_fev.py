import json
import csv
import unicodedata
from processamento_dados import Dados



def normalizar_texto(texto):
  if not isinstance(texto, str):
    return texto
  texto_normalizado = unicodedata.normalize('NFKD', texto)
  texto_ascii = texto_normalizado.encode('ASCII', 'ignore').decode('ASCII')
  return texto_ascii

def normalizar_dados(dados):
  dados_normalizados = []
  for linha in dados:
    linha_normalizada = {normalizar_texto(k): normalizar_texto(v) for k, v in linha.items()}
    dados_normalizados.append(linha_normalizada)
  return dados_normalizados

def leitura_json(path_json):
  dados_json = []
  with open(path_json, 'r') as file:
    dados_json = json.load(file)
  return dados_json

def leitura_csv(path_csv): 
  dados_csv = []
  with open(path_csv, 'r') as file:
    spamreader = csv.DictReader(file, delimiter=',')
    for row in spamreader:
      dados_csv.append(row)
  return dados_csv

def leitura_dados(path, tipo):
  dados = []


  if tipo == 'csv':
    dados = leitura_csv(path)
  elif tipo == 'json':
    dados = leitura_json(path)
  return dados

def get_columns(dados):
    return list(dados[-1].keys())


def rename_columns(dados, key_mapping):
  new_dados_csv = []
  for old_dict in dados:
    dict_temp = {}
    for old_key, value in old_dict.items():
      dict_temp[key_mapping[old_key]] = value
      new_dados_csv.append(dict_temp)
            
  return new_dados_csv


def size_data(dados):
  return len(dados)

def join(dadosA, dadosB):
    combined_list = []
    combined_list.extend(dadosA)
    combined_list.extend(dadosB)
    return combined_list


def transformando_dados_tabela(dados, nomes_colunas):
    
    dados_combinados_tabela = [nomes_colunas]

    for row in dados:
        linha = []
        for coluna in nomes_colunas:
            linha.append(row.get(coluna, 'Indisponivel'))
        dados_combinados_tabela.append(linha)
    
    return dados_combinados_tabela

def salvando_dados(dados, path):
    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(dados)


path_json = '..\data_raw\dados_empresaA.json'
path_csv = '..\data_raw\dados_empresaB.csv'

dados_empresaA = Dados(path_json, 'json')
print(dados_empresaA.path)

# # Iniciando a leitura

# dados_json = leitura_dados(path_json, 'json')
# nome_colunas_json = get_columns(dados_json)
# print(nome_colunas_json)

# dados_csv = leitura_dados(path_csv, 'csv')
# nome_colunas_csv = get_columns(dados_csv)
# print(nome_colunas_csv)

# Exemplo de uso das funções de normalização:
# dados_json = leitura_dados(path_json, 'json')
# dados_json = normalizar_dados(dados_json)
# nome_colunas_json = get_columns(dados_json)
# nome_colunas_json = [normalizar_texto(col) for col in nome_colunas_json]
# print(nome_colunas_json)

# dados_csv = leitura_dados(path_csv, 'csv')
# dados_csv = normalizar_dados(dados_csv)
# nome_colunas_csv = get_columns(dados_csv)
# nome_colunas_csv = [normalizar_texto(col) for col in nome_colunas_csv]
# print(nome_colunas_csv)



# # Transformação dos dados

# key_mapping = {'Nome do Item': 'Nome do Produto',
#                'Classificação do Produto': 'Categoria do Produto',
#                'Valor em Reais (R$)': 'Preço do Produto (R$)',
#                'Quantidade em Estoque': 'Quantidade em Estoque',
#                'Nome da Loja': 'Filial',
#                'Data da Venda': 'Data da Venda'}

# dados_csv = rename_columns(dados_csv, key_mapping)
# nome_colunas_csv = get_columns(dados_csv)
# print(nome_colunas_csv)

# dados_fusao = join(dados_json, dados_csv)
# nomes_colunas_fusao = get_columns(dados_fusao)
# tamanho_dados_fusao = size_data(dados_fusao)
# print(nomes_colunas_fusao)
# print(tamanho_dados_fusao)

# # Salvar os dados

# dados_fusao_tabela = transformando_dados_tabela(dados_fusao, nomes_colunas_fusao)
# path_dados_combinados = '/content/drive/MyDrive/MeuProjetoColab/pipeline-dados/data_processed/dados-combinados.csv'
# salvando_dados(dados_fusao_tabela, path_dados_combinados)
# print(path_dados_combinados)