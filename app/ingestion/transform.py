import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

# Import database models
from app.database.models import Deputado, Despesa, Discurso, Votacao, Voto

logger = logging.getLogger(__name__)

def transform_deputado(data: Dict) -> Deputado:
    """
    Transform deputy data from API to database model format.
    
    Args:
        data: Dictionary with deputy data from API
        
    Returns:
        Deputado: Database model instance
    """
    try:
        # Create base deputado object with handling for different key formats
        deputado = Deputado(
            id=data['id'],
            uri=data.get('uri', ''),
            nome_civil=data.get('nomeCivil', data.get('nome', '')),
            cpf=data.get('cpf'),
            sexo=data.get('sexo'),
            url_website=data.get('urlWebsite', data.get('url_website')),
            data_nascimento=data.get('dataNascimento', data.get('data_nascimento')),
            data_falecimento=data.get('dataFalecimento', data.get('data_falecimento')),
            uf_nascimento=data.get('ufNascimento', data.get('uf_nascimento')),
            municipio_nascimento=data.get('municipioNascimento', data.get('municipio_nascimento'))
        )
        
        # Add ultimo_status fields if present
        ultimo_status = data.get('ultimoStatus', data.get('ultimo_status', {}))
        if ultimo_status:
            deputado.ultimo_status_id = ultimo_status.get('id')
            deputado.ultimo_status_nome = ultimo_status.get('nome')
            deputado.ultimo_status_sigla_partido = ultimo_status.get('siglaPartido', ultimo_status.get('sigla_partido'))
            deputado.ultimo_status_uri_partido = ultimo_status.get('uriPartido', ultimo_status.get('uri_partido'))
            deputado.ultimo_status_sigla_uf = ultimo_status.get('siglaUf', ultimo_status.get('sigla_uf'))
            deputado.ultimo_status_id_legislatura = ultimo_status.get('idLegislatura', ultimo_status.get('id_legislatura'))
            deputado.ultimo_status_url_foto = ultimo_status.get('urlFoto', ultimo_status.get('url_foto'))
            deputado.ultimo_status_email = ultimo_status.get('email')
            deputado.ultimo_status_data = ultimo_status.get('data')
            deputado.ultimo_status_nome_eleitoral = ultimo_status.get('nomeEleitoral', ultimo_status.get('nome_eleitoral'))
            deputado.ultimo_status_gabinete = ultimo_status.get('gabinete')
            deputado.ultimo_status_situacao = ultimo_status.get('situacao')
            deputado.ultimo_status_condicao_eleitoral = ultimo_status.get('condicaoEleitoral', ultimo_status.get('condicao_eleitoral'))
            deputado.ultimo_status_descricao = ultimo_status.get('descricaoStatus', ultimo_status.get('descricao_status'))
        
        # Also try to get values that might be directly at the top level
        if not deputado.ultimo_status_nome:
            deputado.ultimo_status_nome = data.get('nome')
        if not deputado.ultimo_status_sigla_partido:
            deputado.ultimo_status_sigla_partido = data.get('siglaPartido', data.get('sigla_partido'))
        if not deputado.ultimo_status_sigla_uf:
            deputado.ultimo_status_sigla_uf = data.get('siglaUf', data.get('sigla_uf'))
        if not deputado.ultimo_status_email:
            deputado.ultimo_status_email = data.get('email')
        if not deputado.ultimo_status_url_foto:
            deputado.ultimo_status_url_foto = data.get('urlFoto', data.get('url_foto'))
        
        return deputado
        
    except Exception as e:
        logger.error(f"Error transforming deputado data: {e}")
        logger.debug(f"Problematic data: {data}")
        raise

def transform_despesa(data: Dict, deputado_id: int) -> Despesa:
    """
    Transform expense data from API to database model format.
    
    Args:
        data: Dictionary with expense data from API
        deputado_id: ID of the deputy associated with the expense
        
    Returns:
        Despesa: Database model instance
    """
    try:
        return Despesa(
            deputado_id=deputado_id,
            ano=data.get('ano'),
            mes=data.get('mes'),
            tipo_despesa=data.get('tipoDespesa', data.get('tipo_despesa', '')),
            cod_documento=data.get('codDocumento', data.get('cod_documento', 0)),
            tipo_documento=data.get('tipoDocumento', data.get('tipo_documento', '')),
            cod_tipo_documento=data.get('codTipoDocumento', data.get('cod_tipo_documento', 0)),
            data_documento=data.get('dataDocumento', data.get('data_documento')),
            num_documento=data.get('numDocumento', data.get('num_documento', '')),
            valor_documento=data.get('valorDocumento', data.get('valor_documento', 0.0)),
            url_documento=data.get('urlDocumento', data.get('url_documento', '')),
            nome_fornecedor=data.get('nomeFornecedor', data.get('nome_fornecedor', '')),
            cnpj_cpf_fornecedor=data.get('cnpjCpfFornecedor', data.get('cnpj_cpf_fornecedor', '')),
            valor_liquido=data.get('valorLiquido', data.get('valor_liquido', 0.0)),
            valor_glosa=data.get('valorGlosa', data.get('valor_glosa', 0.0)),
            num_ressarcimento=data.get('numRessarcimento', data.get('num_ressarcimento')),
            cod_lote=data.get('codLote', data.get('cod_lote')),
            parcela=data.get('parcela')
        )
    except Exception as e:
        logger.error(f"Error transforming despesa data: {e}")
        logger.debug(f"Problematic data: {data}")
        raise

def transform_discurso(data: Dict, deputado_id: int) -> Discurso:
    """
    Transform speech data from API to database model format.
    
    Args:
        data: Dictionary with speech data from API
        deputado_id: ID of the deputy associated with the speech
        
    Returns:
        Discurso: Database model instance
    """
    try:
        return Discurso(
            deputado_id=deputado_id,
            data_hora_inicio=data.get('dataHoraInicio', data.get('data_hora_inicio')),
            data_hora_fim=data.get('dataHoraFim', data.get('data_hora_fim')),
            fase_evento=data.get('faseEvento', data.get('fase_evento')),
            tipo_discurso=data.get('tipoDiscurso', data.get('tipo_discurso', '')),
            url_texto=data.get('urlTexto', data.get('url_texto')),
            url_audio=data.get('urlAudio', data.get('url_audio')),
            url_video=data.get('urlVideo', data.get('url_video')),
            keywords=data.get('keywords'),
            sumario=data.get('sumario'),
            transcricao=data.get('transcricao')
        )
    except Exception as e:
        logger.error(f"Error transforming discurso data: {e}")
        logger.debug(f"Problematic data: {data}")
        raise

def transform_votacao(data: Dict) -> Votacao:
    """
    Transform voting session data from API to database model format.
    
    Args:
        data: Dictionary with voting session data from API
        
    Returns:
        Votacao: Database model instance
    """
    try:
        # Garantir que temos um ID válido
        if 'id' not in data:
            logger.error("Missing ID in votacao data")
            raise ValueError("Votacao data must have an ID")
            
        return Votacao(
            id=data['id'],
            uri=data.get('uri', ''),
            data=data.get('data'),
            data_hora_registro=data.get('dataHoraRegistro', data.get('data_hora_registro')),
            sigla_orgao=data.get('siglaOrgao', data.get('sigla_orgao', '')),
            uri_orgao=data.get('uriOrgao', data.get('uri_orgao', '')),
            proposicao_objeto=data.get('proposicaoObjeto', data.get('proposicao_objeto')),
            tipo_votacao=data.get('tipoVotacao', data.get('tipo_votacao', {})),
            ultima_apresentacao_proposicao=data.get('ultimaApresentacaoProposicao', 
                                               data.get('ultima_apresentacao_proposicao')),
            aprovacao=data.get('aprovacao', False)
        )
    except Exception as e:
        logger.error(f"Error transforming votacao data: {e}")
        logger.debug(f"Problematic data: {data}")
        raise

def transform_voto(data: Dict, votacao_id: str) -> Voto:
    """
    Transform vote data from API to database model format.
    
    Args:
        data: Dictionary with vote data from API
        votacao_id: ID of the voting session
        
    Returns:
        Voto: Database model instance
    """
    try:
        # Obter o ID do deputado, que pode estar em diferentes locais
        deputado_id = None
        
        # Verificar se existe como campo 'deputado_id'
        if 'deputado_id' in data or 'deputadoId' in data:
            deputado_id = data.get('deputado_id', data.get('deputadoId'))
        
        # Verificar se existe como objeto aninhado 'deputado'
        elif 'deputado' in data and isinstance(data['deputado'], dict):
            deputado_id = data['deputado'].get('id')
        
        # Verificar se existe como objeto aninhado 'deputado_'
        elif 'deputado_' in data and isinstance(data['deputado_'], dict):
            deputado_id = data['deputado_'].get('id')
        
        if not deputado_id:
            logger.warning(f"No deputado_id found in vote data for votacao {votacao_id}")
        
        return Voto(
            votacao_id=votacao_id,
            deputado_id=deputado_id,
            data_registro_voto=data.get('dataRegistroVoto', data.get('data_registro_voto')),
            tipo_voto=data.get('tipoVoto', data.get('tipo_voto', ''))
        )
    except Exception as e:
        logger.error(f"Error transforming voto data: {e}")
        logger.debug(f"Problematic data: {data}")
        raise

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
    
    result = []
    for idx, row in df.iterrows():
        try:
            row_dict = row.to_dict()
            model = transform_func(row_dict, **kwargs)
            result.append(model)
        except Exception as e:
            logger.error(f"Error transforming row {idx}: {e}")
            
    return result
