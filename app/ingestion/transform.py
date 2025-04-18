import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import logging
from prefect import task

# Import database models
from app.database.models import Deputado, Despesa, Discurso, Votacao, Voto
from app.ingestion.utils import save_dataframe, load_dataframe

logger = logging.getLogger(__name__)

# ---- Funções para transformar registros individuais em modelos SQLAlchemy ----

def transform_deputado(data: Union[Dict, pd.DataFrame]) -> Deputado:
    """
    Transform deputy data from API to database model format.
    
    Args:
        data: Dictionary with deputy data from API
        
    Returns:
        Deputado: Database model instance
    """
    try:
        # Handle ultimo_status data
        ultimo_status = data.get('ultimoStatus', data.get('ultimo_status', {}))
        if isinstance(ultimo_status, str) and ultimo_status.startswith('{'):
            # Handle case where ultimo_status might be a JSON string
            import json
            try:
                ultimo_status = json.loads(ultimo_status)
            except:
                ultimo_status = {}
        
        # Handle gabinete data properly as JSON field
        gabinete = ultimo_status.get('gabinete', {})
        
        # Create the deputado object with only valid fields from the schema
        deputado = Deputado(
            id=data['id'],
            uri=data.get('uri', ''),
            nome_civil=data.get('nomeCivil', data.get('nome_civil', data.get('nome', ''))),
            cpf=data.get('cpf'),
            sexo=data.get('sexo'),
            escolaridade=data.get('escolaridade'),
            url_website=data.get('urlWebsite', data.get('url_website')),
            data_nascimento=data.get('dataNascimento', data.get('data_nascimento')),
            data_falecimento=data.get('dataFalecimento', data.get('data_falecimento')),
            uf_nascimento=data.get('ufNascimento', data.get('uf_nascimento')),
            municipio_nascimento=data.get('municipioNascimento', data.get('municipio_nascimento')),
            
            # UltimoStatus fields
            ultimo_status_id=ultimo_status.get('id'),
            ultimo_status_nome=ultimo_status.get('nome', data.get('nome')),
            ultimo_status_sigla_partido=ultimo_status.get('siglaPartido', data.get('siglaPartido')),
            ultimo_status_uri_partido=ultimo_status.get('uriPartido', data.get('uriPartido')),
            ultimo_status_sigla_uf=ultimo_status.get('siglaUf', data.get('siglaUf')),
            ultimo_status_id_legislatura=ultimo_status.get('idLegislatura', data.get('idLegislatura')),
            ultimo_status_url_foto=ultimo_status.get('urlFoto', data.get('urlFoto')),
            ultimo_status_email=ultimo_status.get('email', data.get('email')),
            ultimo_status_data=ultimo_status.get('data'),
            ultimo_status_nome_eleitoral=ultimo_status.get('nomeEleitoral'),
            ultimo_status_situacao=ultimo_status.get('situacao'),
            ultimo_status_condicao_eleitoral=ultimo_status.get('condicaoEleitoral'),
            ultimo_status_descricao=ultimo_status.get('descricaoStatus'),
            
            # Gabinete is a JSON field, not individual fields
            ultimo_status_gabinete=gabinete
        )
        
        # Alternative field sources if primary fields are not available
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

def transform_despesa(data: Union[Dict, pd.DataFrame], deputado_id: int) -> Despesa:
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

def transform_discurso(data: Union[Dict, pd.DataFrame], deputado_id: int) -> Discurso:
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

def transform_votacao(data: Union[Dict, pd.DataFrame]) -> Votacao:
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

def transform_voto(data: Union[Dict, pd.DataFrame], votacao_id: str) -> Voto:
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

# ---- Tarefas Prefect para transformação de DataFrames ----

@task(name="Transform Deputados")
def transform_deputados(df_deputados: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Transform deputies data from API format to a format suitable for database loading.
    
    Args:
        df_deputados: Raw DataFrame with deputies data
        
    Returns:
        Transformed DataFrame ready for database loading
    """
    if df_deputados is None:
        df_deputados = load_dataframe("deputados")
    
    if df_deputados is None or df_deputados.empty:
        logger.warning("No deputados data available for transformation")
        return None
    
    logger.info(f"Transforming {len(df_deputados)} deputados")
    
    # Create result DataFrame with the structure matching the database model
    result = []
    
    for _, row in df_deputados.iterrows():
        try:
            # Extract ultimo_status data from the nested JSON or dict
            ultimo_status = row.get('ultimo_status', {})
            if isinstance(ultimo_status, str):
                # Handle case where ultimo_status might be a JSON string
                try:
                    import json
                    ultimo_status = json.loads(ultimo_status)
                except:
                    ultimo_status = {}
            
            # Direct conversion from API fields to database fields
            deputado_dict = {
                # Basic fields
                'id': row['id'],
                'uri': row.get('uri', ''),
                'nome_civil': row.get('nomeCivil', row.get('nome', '')),
                'cpf': row.get('cpf'),
                'sexo': row.get('sexo'),
                'escolaridade': row.get('escolaridade'),
                'url_website': row.get('urlWebsite', row.get('url_website')),
                'data_nascimento': row.get('dataNascimento', row.get('data_nascimento')),
                'data_falecimento': row.get('dataFalecimento', row.get('data_falecimento')),
                'uf_nascimento': row.get('ufNascimento', row.get('uf_nascimento')),
                'municipio_nascimento': row.get('municipioNascimento', row.get('municipio_nascimento')),
                
                # UltimoStatus fields
                'ultimo_status_id': ultimo_status.get('id'),
                'ultimo_status_nome': ultimo_status.get('nome', row.get('nome')),
                'ultimo_status_sigla_partido': ultimo_status.get('siglaPartido', row.get('siglaPartido')),
                'ultimo_status_uri_partido': ultimo_status.get('uriPartido', row.get('uriPartido')),
                'ultimo_status_sigla_uf': ultimo_status.get('siglaUf', row.get('siglaUf')),
                'ultimo_status_id_legislatura': ultimo_status.get('idLegislatura', row.get('idLegislatura')),
                'ultimo_status_url_foto': ultimo_status.get('urlFoto', row.get('urlFoto')),
                'ultimo_status_email': ultimo_status.get('email', row.get('email')),
                'ultimo_status_data': ultimo_status.get('data'),
                'ultimo_status_nome_eleitoral': ultimo_status.get('nomeEleitoral'),
                'ultimo_status_situacao': ultimo_status.get('situacao'),
                'ultimo_status_condicao_eleitoral': ultimo_status.get('condicaoEleitoral'),
                'ultimo_status_descricao': ultimo_status.get('descricaoStatus')
            }
            
            # Handle gabinete data if present
            gabinete = ultimo_status.get('gabinete', {})
            if gabinete and isinstance(gabinete, dict):
                deputado_dict['ultimo_status_gabinete'] = gabinete
            
            result.append(deputado_dict)
        except Exception as e:
            logger.error(f"Error transforming deputado record: {e}")
    
    # Convert to DataFrame
    df_clean = pd.DataFrame(result)
    
    # Save processed data
    save_dataframe(df_clean, "deputados", processed=True)
    
    return df_clean

@task(name="Transform Votacoes")
def transform_votacoes(df_votacoes: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Transform voting sessions data.
    
    Args:
        df_votacoes: Raw DataFrame with voting sessions
        
    Returns:
        Transformed DataFrame
    """
    if df_votacoes is None:
        df_votacoes = load_dataframe("votacoes")
    
    if df_votacoes is None or df_votacoes.empty:
        logger.warning("No votacoes data available for transformation")
        return None
    
    logger.info(f"Transforming {len(df_votacoes)} votacoes")
    
    try:
        # Check for required ID column
        if 'id' not in df_votacoes.columns:
            logger.error("Missing required 'id' column in votacoes data")
            return None
        
        # Select necessary columns with fallbacks for column names
        df_clean = pd.DataFrame()
        df_clean['id'] = df_votacoes['id']
        
        # Map of expected columns to possible API column names
        column_map = {
            'uri': ['uri'],
            'data': ['data'],
            'dataHoraRegistro': ['dataHoraRegistro', 'data_hora_registro'],
            'siglaOrgao': ['siglaOrgao', 'sigla_orgao'],
            'uriOrgao': ['uriOrgao', 'uri_orgao'],
            'proposicaoObjeto': ['proposicaoObjeto', 'proposicao_objeto'],
            'tipoVotacao': ['tipoVotacao', 'tipo_votacao'],
            'ultimaApresentacaoProposicao': ['ultimaApresentacaoProposicao', 'ultima_apresentacao_proposicao']
        }
        
        # Try to find each column in the dataframe
        for col, possible_names in column_map.items():
            for name in possible_names:
                if name in df_votacoes.columns:
                    df_clean[col] = df_votacoes[name]
                    break
            else:
                # Column not found, set to None
                df_clean[col] = None
                
        # Add aprovacao column (if exists in the description or API data)
        if 'aprovacao' in df_votacoes.columns:
            df_clean['aprovacao'] = df_votacoes['aprovacao']
        else:
            # Try to derive from descricao field if available
            if 'descricao' in df_votacoes.columns:
                df_clean['aprovacao'] = df_votacoes['descricao'].str.contains('Aprovad[ao]', regex=True, case=False)
            else:
                df_clean['aprovacao'] = False
        
        # Convert data types safely
        df_clean = df_clean.convert_dtypes()
        
        # Save processed data
        save_dataframe(df_clean, "votacoes", processed=True)
        
        return df_clean
    
    except Exception as e:
        logger.error(f"Error transforming votacoes: {e}")
        # Return a minimal dataframe with just the IDs if possible
        if 'id' in df_votacoes.columns:
            return pd.DataFrame({'id': df_votacoes['id']})
        return None

@task(name="Transform Votos")
def transform_votos(df_votos: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Transform votes data.
    
    Args:
        df_votos: Raw DataFrame with votes
        
    Returns:
        Transformed DataFrame
    """
    if df_votos is None:
        df_votos = load_dataframe("votos")
    
    if df_votos is None or df_votos.empty:
        logger.warning("No votos data available for transformation")
        return None
    
    logger.info(f"Transforming {len(df_votos)} votos")
    
    try:
        # Create the clean dataframe
        df_clean = pd.DataFrame()
        
        # Check for required columns
        required_columns = {
            'idVotacao': ['idVotacao', 'votacao_id', 'id_votacao'],
            'tipoVoto': ['tipoVoto', 'tipo_voto'],
            'dataRegistroVoto': ['dataRegistroVoto', 'data_registro_voto']
        }
        
        # Try to add each required column
        for col, possible_names in required_columns.items():
            found = False
            for name in possible_names:
                if name in df_votos.columns:
                    df_clean[col] = df_votos[name]
                    found = True
                    break
            
            if not found:
                logger.warning(f"Required column {col} not found in votos data")
                if col == 'idVotacao' and len(df_votos) > 0:
                    # Try to use the index as a fallback
                    df_clean[col] = "unknown"
                else:
                    df_clean[col] = None
        
        # Extract deputado ID from possibly nested structures
        df_clean['deputadoId'] = None
        
        # Caso 1: O ID do deputado está diretamente disponível
        if 'deputadoId' in df_votos.columns:
            df_clean['deputadoId'] = df_votos['deputadoId']
        elif 'deputado_id' in df_votos.columns:
            df_clean['deputadoId'] = df_votos['deputado_id']
        
        # Caso 2: O ID do deputado está em uma coluna aninhada como 'deputado_'
        elif 'deputado_' in df_votos.columns:
            # Tentar extrair o ID do objeto aninhado
            def extract_deputado_id(dep_obj):
                if isinstance(dep_obj, dict):
                    return dep_obj.get('id')
                return None
            
            df_clean['deputadoId'] = df_votos['deputado_'].apply(extract_deputado_id)
        
        # Caso 3: O ID do deputado está em uma coluna aninhada como 'deputado'
        elif 'deputado' in df_votos.columns:
            def extract_deputado_id(dep_obj):
                if isinstance(dep_obj, dict):
                    return dep_obj.get('id')
                return None
            
            df_clean['deputadoId'] = df_votos['deputado'].apply(extract_deputado_id)
            
        # Convert data types
        df_clean = df_clean.convert_dtypes()
        
        # Save processed data
        save_dataframe(df_clean, "votos", processed=True)
        
        return df_clean
    
    except Exception as e:
        logger.error(f"Error transforming votos: {e}", exc_info=True)
        # Return minimal dataframe if possible
        try:
            minimal_df = pd.DataFrame()
            if 'idVotacao' in df_votos.columns:
                minimal_df['idVotacao'] = df_votos['idVotacao']
            elif 'votacao_id' in df_votos.columns:
                minimal_df['idVotacao'] = df_votos['votacao_id']
                
            return minimal_df
        except:
            return None
        
@task(name="Transform Discursos")
def transform_discursos(df_discursos: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Transform speeches data.

    Args:
        df_discursos: Raw DataFrame with speeches data

    Returns:
        Transformed DataFrame
    """
    if df_discursos is None:
        df_discursos = load_dataframe("discursos")

    if df_discursos is None or df_discursos.empty:
        logger.warning("No discursos data available for transformation")
        return None
    
    logger.info(f"Transforming {len(df_discursos)} discursos")

    try:
        df_clean = pd.DataFrame()

        # Check for required ID column
        required_columns = {
            'id': ['id'],
            'deputado_id': ['deputado_id', 'deputadoId', 'idDeputado'],
            'data_hora_inicio': ['dataHoraInicio', 'data_hora_inicio'],
            'data_hora_fim': ['dataHoraFim', 'data_hora_fim'],
            'fase_evento': ['faseEvento', 'fase_evento'],
            'tipo_discurso': ['tipoDiscurso', 'tipo_discurso'],
            'url_texto': ['urlTexto', 'url_texto'],
            'url_audio': ['urlAudio', 'url_audio'],
            'url_video': ['urlVideo', 'url_video'],
            'keywords': ['keywords'],
            'sumario': ['sumario'],
            'transcricao': ['transcricao']
        }

        # Try to add each required column
        for col, possible_names in required_columns.items():
            for name in possible_names:
                if name in df_discursos.columns:
                    df_clean[col] = df_discursos[name]
                    break
            else:
                df_clean[col] = None

        # Convert data types safely
        df_clean = df_clean.convert_dtypes()

        # Save processed data
        save_dataframe(df_clean, "discursos", processed=True)

        return df_clean
    
    except Exception as e:
        logger.error(f"Error transforming discursos: {e}", exc_info=True)
        # Return a minimal dataframe with just the IDs if possible
        if 'id' in df_discursos.columns:
            return pd.DataFrame({'id': df_discursos['id']})
        return None
