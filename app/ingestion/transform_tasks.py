import logging
from typing import Dict, List, Optional, Any
import pandas as pd
from prefect import task

from app.ingestion.utils import save_dataframe, load_dataframe

logger = logging.getLogger(__name__)

@task(name="Transform Deputados")
def transform_deputados(df_deputados: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Transform deputies data.
    
    Args:
        df_deputados: Raw DataFrame with deputies data
        
    Returns:
        Transformed DataFrame
    """
    if df_deputados is None:
        df_deputados = load_dataframe("deputados")
    
    if df_deputados is None or df_deputados.empty:
        logger.warning("No deputados data available for transformation")
        return None
    
    logger.info(f"Transforming {len(df_deputados)} deputados")
    
    try:
        # Select necessary columns (using get to handle missing columns)
        columns = ['id', 'uri', 'nome', 'siglaPartido', 'siglaUf', 'idLegislatura', 'urlFoto', 'email']
        df_clean = pd.DataFrame()
        
        # Add columns safely
        for col in columns:
            if col in df_deputados.columns:
                df_clean[col] = df_deputados[col]
            else:
                # Try alternative column names using snake_case
                snake_case = ''.join(['_' + c.lower() if c.isupper() else c for c in col]).lstrip('_')
                if snake_case in df_deputados.columns:
                    df_clean[col] = df_deputados[snake_case]
                else:
                    logger.warning(f"Column {col} not found, setting to empty")
                    df_clean[col] = None
        
        # Ensure ID is always present
        if 'id' not in df_clean.columns or df_clean['id'].isnull().all():
            raise ValueError("ID column is missing or all null")
            
        # Convert data types safely
        df_clean = df_clean.convert_dtypes()
        
        # Save processed data
        save_dataframe(df_clean, "deputados", processed=True)
        
        return df_clean
    
    except Exception as e:
        logger.error(f"Error transforming deputados: {e}")
        # Return a minimal dataframe with just the IDs if possible
        if 'id' in df_deputados.columns:
            return pd.DataFrame({'id': df_deputados['id']})
        return None

@task(name="Transform Deputados Details")
def transform_deputados_details(df_detalhes: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Transform deputies details data.
    
    Args:
        df_detalhes: Raw DataFrame with deputies details
        
    Returns:
        Transformed DataFrame
    """
    if df_detalhes is None:
        df_detalhes = load_dataframe("detalhes_deputados")
    
    if df_detalhes is None or df_detalhes.empty:
        logger.warning("No deputados details data available for transformation")
        return None
    
    logger.info(f"Transforming {len(df_detalhes)} deputados details")
    
    try:
        # Create a new DataFrame with required columns
        df_clean = pd.DataFrame()
        
        # Always ensure ID is present
        if 'id' in df_detalhes.columns:
            df_clean['id'] = df_detalhes['id']
        else:
            logger.error("ID column missing from detalhes_deputados")
            return None
        
        # Copy basic columns with fallbacks
        columns = {
            'nomeCivil': 'nomeCivil', 
            'dataNascimento': 'dataNascimento', 
            'municipioNascimento': 'municipioNascimento', 
            'ufNascimento': 'ufNascimento', 
            'escolaridade': 'escolaridade', 
            'sexo': 'sexo'
        }
        
        for col, api_col in columns.items():
            if api_col in df_detalhes.columns:
                df_clean[col] = df_detalhes[api_col]
            else:
                # Try snake_case version
                snake_case = ''.join(['_' + c.lower() if c.isupper() else c for c in api_col]).lstrip('_')
                if snake_case in df_detalhes.columns:
                    df_clean[col] = df_detalhes[snake_case]
                else:
                    df_clean[col] = None
        
        # Handle ultimoStatus column which might be a nested dict
        if 'ultimoStatus' in df_detalhes.columns:
            # Check if it's a Series of dicts or already a DataFrame
            if isinstance(df_detalhes['ultimoStatus'].iloc[0], dict):
                ultimo_status_df = pd.json_normalize(df_detalhes['ultimoStatus'])
                
                # Extract relevant fields
                if 'condicaoEleitoral' in ultimo_status_df.columns:
                    df_clean['condicaoEleitoral'] = ultimo_status_df['condicaoEleitoral']
                if 'situacao' in ultimo_status_df.columns:
                    df_clean['situacao'] = ultimo_status_df['situacao']
                if 'data' in ultimo_status_df.columns:
                    df_clean['dataUltimoStatus'] = ultimo_status_df['data']
                if 'gabinete.telefone' in ultimo_status_df.columns:
                    df_clean['telefone'] = ultimo_status_df['gabinete.telefone']
                if 'siglaPartido' in ultimo_status_df.columns:
                    df_clean['siglaPartido'] = ultimo_status_df['siglaPartido']
        
        # Convert data types safely
        df_clean = df_clean.convert_dtypes()
        
        # Save processed data
        save_dataframe(df_clean, "detalhes_deputados", processed=True)
        
        return df_clean
    
    except Exception as e:
        logger.error(f"Error transforming deputados details: {e}")
        # Return a minimal dataframe with just the IDs if possible
        if 'id' in df_detalhes.columns:
            return pd.DataFrame({'id': df_detalhes['id']})
        return None

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