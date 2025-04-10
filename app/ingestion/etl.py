import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, Any

from .load import (
    load_deputados, load_despesas, load_discursos, 
    load_votacoes, load_votos, run_full_etl
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Run ETL process for Câmara dos Deputados data')
    
    parser.add_argument(
        '--mode',
        choices=['full', 'incremental'],
        default='incremental',
        help='ETL mode: full (delete and reload all data) or incremental (update only)'
    )
    
    parser.add_argument(
        '--entity',
        choices=['all', 'deputados', 'despesas', 'discursos', 'votacoes', 'votos'],
        default='all',
        help='Entity to process (default: all)'
    )
    
    parser.add_argument(
        '--deputado-id',
        type=int,
        help='ID of the deputy (required for despesas, discursos, or votos when entity is specific)'
    )
    
    parser.add_argument(
        '--votacao-id',
        type=str,
        help='ID of the voting session (required for votos when entity is specific)'
    )
    
    parser.add_argument(
        '--inicio',
        type=str,
        help='Start date in YYYY-MM-DD format for filtering data'
    )
    
    parser.add_argument(
        '--fim',
        type=str,
        help='End date in YYYY-MM-DD format for filtering data'
    )
    
    parser.add_argument(
        '--legislatura',
        type=int,
        help='ID of the legislature for filtering deputies'
    )
    
    parser.add_argument(
        '--partido',
        type=str,
        help='Party acronym for filtering deputies'
    )
    
    parser.add_argument(
        '--uf',
        type=str,
        help='State acronym for filtering deputies'
    )
    
    return parser.parse_args()

def build_api_params(args) -> Dict[str, Any]:
    """Build API parameters from command line arguments"""
    params = {}
    
    if args.inicio:
        params['dataInicio'] = args.inicio
    
    if args.fim:
        params['dataFim'] = args.fim
    
    if args.legislatura:
        params['idLegislatura'] = args.legislatura
    
    if args.partido:
        params['siglaPartido'] = args.partido
    
    if args.uf:
        params['siglaUf'] = args.uf
    
    return params

def main():
    """Main ETL function"""
    args = parse_args()
    incremental = args.mode == 'incremental'
    api_params = build_api_params(args)
    
    logger.info(f"Starting ETL process in {args.mode} mode for {args.entity}")
    logger.info(f"Parameters: {api_params}")
    
    try:
        if args.entity == 'all':
            results = run_full_etl(incremental, **api_params)
        
        elif args.entity == 'deputados':
            count = load_deputados(incremental=incremental, **api_params)
            results = {'deputados': count}
        
        elif args.entity == 'despesas':
            if not args.deputado_id:
                raise ValueError("--deputado-id is required for processing expenses")
            count = load_despesas(args.deputado_id, incremental=incremental, **api_params)
            results = {'despesas': count}
        
        elif args.entity == 'discursos':
            if not args.deputado_id:
                raise ValueError("--deputado-id is required for processing speeches")
            count = load_discursos(args.deputado_id, incremental=incremental, **api_params)
            results = {'discursos': count}
        
        elif args.entity == 'votacoes':
            count = load_votacoes(incremental=incremental, **api_params)
            results = {'votacoes': count}
        
        elif args.entity == 'votos':
            if not args.votacao_id:
                raise ValueError("--votacao-id is required for processing votes")
            count = load_votos(args.votacao_id, incremental=incremental)
            results = {'votos': count}
        
        logger.info(f"ETL process completed successfully.")
        logger.info(f"Summary: {results}")
    
    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}")
        raise

if __name__ == "__main__":
    main()
