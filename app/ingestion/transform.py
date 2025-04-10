import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional

# Import database models
from app.database.models import Deputado, Despesa, Discurso, Votacao, Voto

def transform_deputado(data: Dict) -> Deputado:
    """
    Transform deputy data from API to database model format.
    
    Args:
        data: Dictionary with deputy data from API
        
    Returns:
        Deputado: Database model instance
    """
    # Create base deputado object
    deputado = Deputado(
        id=data['id'],
        uri=data['uri'],
        nome_civil=data['nomeCivil'],
        cpf=data.get('cpf'),
        sexo=data.get('sexo'),
        url_website=data.get('urlWebsite'),
        data_nascimento=data.get('dataNascimento'),
        data_falecimento=data.get('dataFalecimento'),
        uf_nascimento=data.get('ufNascimento'),
        municipio_nascimento=data.get('municipioNascimento')
    )
    
    # Add ultimo_status fields if present
    ultimo_status = data.get('ultimoStatus')
    if ultimo_status:
        deputado.ultimo_status_id = ultimo_status.get('id')
        deputado.ultimo_status_nome = ultimo_status.get('nome')
        deputado.ultimo_status_sigla_partido = ultimo_status.get('siglaPartido')
        deputado.ultimo_status_uri_partido = ultimo_status.get('uriPartido')
        deputado.ultimo_status_sigla_uf = ultimo_status.get('siglaUf')
        deputado.ultimo_status_id_legislatura = ultimo_status.get('idLegislatura')
        deputado.ultimo_status_url_foto = ultimo_status.get('urlFoto')
        deputado.ultimo_status_email = ultimo_status.get('email')
        deputado.ultimo_status_data = ultimo_status.get('data')
        deputado.ultimo_status_nome_eleitoral = ultimo_status.get('nomeEleitoral')
        deputado.ultimo_status_gabinete = ultimo_status.get('gabinete')
        deputado.ultimo_status_situacao = ultimo_status.get('situacao')
        deputado.ultimo_status_condicao_eleitoral = ultimo_status.get('condicaoEleitoral')
        deputado.ultimo_status_descricao = ultimo_status.get('descricaoStatus')
    
    return deputado

def transform_despesa(data: Dict, deputado_id: int) -> Despesa:
    """
    Transform expense data from API to database model format.
    
    Args:
        data: Dictionary with expense data from API
        deputado_id: ID of the deputy associated with the expense
        
    Returns:
        Despesa: Database model instance
    """
    return Despesa(
        deputado_id=deputado_id,
        ano=data['ano'],
        mes=data['mes'],
        tipo_despesa=data['tipoDespesa'],
        cod_documento=data['codDocumento'],
        tipo_documento=data['tipoDocumento'],
        cod_tipo_documento=data['codTipoDocumento'],
        data_documento=data['dataDocumento'],
        num_documento=data['numDocumento'],
        valor_documento=data['valorDocumento'],
        url_documento=data['urlDocumento'],
        nome_fornecedor=data['nomeFornecedor'],
        cnpj_cpf_fornecedor=data['cnpjCpfFornecedor'],
        valor_liquido=data['valorLiquido'],
        valor_glosa=data['valorGlosa'],
        num_ressarcimento=data.get('numRessarcimento'),
        cod_lote=data.get('codLote'),
        parcela=data.get('parcela')
    )

def transform_discurso(data: Dict, deputado_id: int) -> Discurso:
    """
    Transform speech data from API to database model format.
    
    Args:
        data: Dictionary with speech data from API
        deputado_id: ID of the deputy associated with the speech
        
    Returns:
        Discurso: Database model instance
    """
    return Discurso(
        deputado_id=deputado_id,
        data_hora_inicio=data['dataHoraInicio'],
        data_hora_fim=data.get('dataHoraFim'),
        fase_evento=data.get('faseEvento'),
        tipo_discurso=data['tipoDiscurso'],
        url_texto=data.get('urlTexto'),
        url_audio=data.get('urlAudio'),
        url_video=data.get('urlVideo'),
        keywords=data.get('keywords'),
        sumario=data.get('sumario'),
        transcricao=data.get('transcricao')
    )

def transform_votacao(data: Dict) -> Votacao:
    """
    Transform voting session data from API to database model format.
    
    Args:
        data: Dictionary with voting session data from API
        
    Returns:
        Votacao: Database model instance
    """
    return Votacao(
        id=data['id'],
        uri=data['uri'],
        data=data['data'],
        data_hora_registro=data['dataHoraRegistro'],
        sigla_orgao=data['siglaOrgao'],
        uri_orgao=data['uriOrgao'],
        proposicao_objeto=data.get('proposicaoObjeto'),
        tipo_votacao=data['tipoVotacao'],
        ultima_apresentacao_proposicao=data.get('ultimaApresentacaoProposicao'),
        aprovacao=data.get('aprovacao', False)
    )

def transform_voto(data: Dict, votacao_id: str) -> Voto:
    """
    Transform vote data from API to database model format.
    
    Args:
        data: Dictionary with vote data from API
        votacao_id: ID of the voting session
        
    Returns:
        Voto: Database model instance
    """
    deputado_data = data.get('deputado', {})
    deputado_id = deputado_data.get('id')
    
    return Voto(
        votacao_id=votacao_id,
        deputado_id=deputado_id,
        data_registro_voto=data['dataRegistroVoto'],
        tipo_voto=data['tipoVoto']
    )

def transform_dataframe_to_models(df: pd.DataFrame, transform_func, **kwargs) -> List:
    """
    Transform a pandas DataFrame to a list of database model instances.
    
    Args:
        df: DataFrame with data from API
        transform_func: Function to transform each row
        **kwargs: Additional arguments to pass to transform_func
        
    Returns:
        List of database model instances
    """
    if df is None or df.empty:
        return []
    
    return [transform_func(row.to_dict(), **kwargs) for _, row in df.iterrows()]
