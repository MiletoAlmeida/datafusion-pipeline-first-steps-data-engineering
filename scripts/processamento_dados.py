import json
import csv

class Dados:
    def __init__(self, path, tipo_dados):
        self._path = path
        self._tipo_dados = tipo_dados
        self._dados = self._leitura_dados()
        self._nome_colunas = self._get_columns()
        self._qtd_linhas = len(self._dados)

    def _leitura_json(self):
        with open(self._path, 'r', encoding='utf-8') as file:
            return json.load(file)

    def _leitura_csv(self):
        with open(self._path, 'r', encoding='utf-8') as file:
            spamreader = csv.DictReader(file, delimiter=',')
            return [row for row in spamreader]

    def _leitura_dados(self):
        if self._tipo_dados == 'csv':
            return self._leitura_csv()
        elif self._tipo_dados == 'json':
            return self._leitura_json()
        elif self._tipo_dados == 'list':
            return self._path
        return []

    def _get_columns(self):
        return list(self._dados[-1].keys()) if self._dados else []

    @property
    def dados(self):
        return self._dados

    @property
    def nome_colunas(self):
        return self._nome_colunas

    @property
    def qtd_linhas(self):
        return self._qtd_linhas

    def rename_columns(self, key_mapping):
        new_dados = []
        for old_dict in self._dados:
            dict_temp = {}
            for old_key, value in old_dict.items():
                if old_key in key_mapping:
                    dict_temp[key_mapping[old_key]] = value
                else:
                    dict_temp[old_key] = value
            new_dados.append(dict_temp)
        self._dados = new_dados
        self._nome_colunas = self._get_columns()
        self._qtd_linhas = len(self._dados)

    @staticmethod
    def join(dadosA, dadosB):
        combined_list = []
        combined_list.extend(dadosA.dados)
        combined_list.extend(dadosB.dados)
        return Dados(combined_list, 'list')

    def transformando_dados_tabela(self):
        dados_combinados_tabela = [self._nome_colunas]
        for row in self._dados:
            linha = []
            for coluna in self._nome_colunas:
                linha.append(row.get(coluna, 'Indisponivel'))
            dados_combinados_tabela.append(linha)
        return dados_combinados_tabela

    def salvando_dados(self, path):
        dados_combinados_tabela = self.transformando_dados_tabela()
        with open(path, 'w', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(dados_combinados_tabela)