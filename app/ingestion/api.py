import requests
import pandas  as pd

def fazer_requisicao(url, parametros=None, returnar_df=True):

    resposta = requests.get(url, params=parametros)
    if resposta.status_code == 200:
        dados = resposta.json()
        if returnar_df and 'dados' in dados and type(dados['dados']) == list: 
            return pd.DataFrame(dados['dados'])
        else:
           return dados
        
    return None

def obter_deputados(id=None, idLegislatura=None, siglaUf=None, siglaPartido=None, nome=None, ordem=None, ordenarPor=None):
    """
    Retorna uma lista de deputados ou informações de um deputado específico.

    Parâmetros:
        - id (int): ID do deputado.
        - idLegislatura (int): ID da legislatura.
        - siglaUf (str): Sigla da unidade federativa.
        - siglaPartido (str): Sigla do partido.
        - nome (str): Nome do deputado.
        - ordem (str): Ordem dos resultados ('asc' ou 'desc').
        - ordenarPor (str): Campo para ordenar os resultados.

    Retorna:
        - dict ou DataFrame: Dados dos deputados ou None se a requisição falhar.
    """
    url = 'https://dadosabertos.camara.leg.br/api/v2/deputados'
    parametros = {}

    if id:
        url += f'/{id}'
    if idLegislatura:
        parametros['idLegislatura'] = idLegislatura
    if siglaUf:
        parametros['siglaUf'] = siglaUf
    if siglaPartido:
        parametros['siglaPartido'] = siglaPartido
    if nome:
        parametros['nome'] = nome
    if ordem:
        parametros['ordem'] = ordem
    if ordenarPor:
        parametros['ordenarPor'] = ordenarPor

    return fazer_requisicao(url, parametros)

def obter_detalhe_deputado(id):
    """
    Retorna informações detalhadas sobre um deputado específico.

    Parâmetros:
        - id (int): ID do deputado.

    Retorna:
        - dict: Dados detalhados do deputado ou None se a requisição falhar.
    """
    url = f'https://dadosabertos.camara.leg.br/api/v2/deputados/{id}'
    return fazer_requisicao(url, returnar_df=False)

def obter_despesas_deputado(id, ano=None, mes=None, cnpjCpfFornecedor=None, ordem=None, ordenarPor=None):
    """
    Retorna as despesas de um deputado.

    Parâmetros:
        - id (int): ID do deputado.
        - ano (int): Ano das despesas.
        - mes (int): Mês das despesas.
        - cnpjCpfFornecedor (str): CNPJ ou CPF do fornecedor.
        - ordem (str): Ordem dos resultados ('asc' ou 'desc').
        - ordenarPor (str): Campo para ordenar os resultados.

    Retorna:
        - dict ou DataFrame: Dados das despesas ou None se a requisição falhar.
    """
    url = f'https://dadosabertos.camara.leg.br/api/v2/deputados/{id}/despesas'
    parametros = {}

    if ano:
        parametros['ano'] = ano
    if mes:
        parametros['mes'] = mes
    if cnpjCpfFornecedor:
        parametros['cnpjCpfFornecedor'] = cnpjCpfFornecedor
    if ordem:
        parametros['ordem'] = ordem
    if ordenarPor:
        parametros['ordenarPor'] = ordenarPor

    return fazer_requisicao(url, parametros)

def obter_discursos_deputado(id, dataInicio=None, dataFim=None, ordem=None, ordenarPor=None):
    """
    Retorna os discursos de um deputado.

    Parâmetros:
        - id (int): ID do deputado.
        - dataInicio (str): Data de início no formato 'YYYY-MM-DD'.
        - dataFim (str): Data de fim no formato 'YYYY-MM-DD'.
        - ordem (str): Ordem dos resultados ('asc' ou 'desc').
        - ordenarPor (str): Campo para ordenar os resultados.

    Retorna:
        - dict ou DataFrame: Dados dos discursos ou None se a requisição falhar.
    """
    url = f'https://dadosabertos.camara.leg.br/api/v2/deputados/{id}/discursos'
    parametros = {}

    if dataInicio:
        parametros['dataInicio'] = dataInicio
    if dataFim:
        parametros['dataFim'] = dataFim
    if ordem:
        parametros['ordem'] = ordem
    if ordenarPor:
        parametros['ordenarPor'] = ordenarPor

    return fazer_requisicao(url, parametros)

def obter_historico_deputado(id):
    """
    Retorna o histórico parlamentar de um deputado.

    Parâmetros:
        - id (int): ID do deputado.

    Retorna:
        - dict ou DataFrame: Dados do histórico parlamentar ou None se a requisição falhar.
    """
    url = f'https://dadosabertos.camara.leg.br/api/v2/deputados/{id}/historico'
    return fazer_requisicao(url)

def obter_votacoes(id=None, idProposicao=None, dataInicio=None, dataFim=None, idTipoVotacao=None, idSituacaoVotacao=None, idOrgao=None, ordem=None, ordenarPor=None):
    """
    Retorna uma lista de votações.

    Parâmetros:
        - id (int): ID da votação.
        - idProposicao (int): ID da proposição relacionada.
        - dataInicio (str): Data de início no formato 'YYYY-MM-DD'.
        - dataFim (str): Data de fim no formato 'YYYY-MM-DD'.
        - idTipoVotacao (int): ID do tipo de votação.
        - idSituacaoVotacao (int): ID da situação da votação.
        - idOrgao (int): ID do órgão relacionado à votação.
        - ordem (str): Ordem dos resultados ('asc' ou 'desc').
        - ordenarPor (str): Campo para ordenar os resultados.

    Retorna:
        - dict ou DataFrame: Dados das votações ou None se a requisição falhar.
    """
    url = 'https://dadosabertos.camara.leg.br/api/v2/votacoes'
    parametros = {}

    if id:
        url += f'/{id}'
    if idProposicao:
        parametros['idProposicao'] = idProposicao
    if dataInicio:
        parametros['dataInicio'] = dataInicio
    if dataFim:
        parametros['dataFim'] = dataFim
    if idTipoVotacao:
        parametros['idTipoVotacao'] = idTipoVotacao
    if idSituacaoVotacao:
        parametros['idSituacaoVotacao'] = idSituacaoVotacao
    if idOrgao:
        parametros['idOrgao'] = idOrgao
    if ordem:
        parametros['ordem'] = ordem
    if ordenarPor:
        parametros['ordenarPor'] = ordenarPor

    return fazer_requisicao(url, parametros)

def obter_votos_votacao(idVotacao: int) -> pd.DataFrame:
    """
    Retorna os votos de uma votação específica.

    Parâmetros:
        - idVotacao (int): ID da votação.

    Retorna:
        - pd.DataFrame: Dados dos votos ou None se a requisição falhar.
    """
    url = f'https://dadosabertos.camara.leg.br/api/v2/votacoes/{idVotacao}/votos'
    return fazer_requisicao(url)