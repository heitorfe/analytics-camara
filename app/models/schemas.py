from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field
from datetime import datetime, date


# Models for /deputados
class UltimoStatus(BaseModel):
    id: int
    uri: str
    nome: str
    siglaPartido: Optional[str] = None
    uriPartido: Optional[str] = None
    siglaUf: str
    idLegislatura: int
    urlFoto: str
    email: Optional[str] = None
    data: Optional[date] = None
    nomeEleitoral: str
    gabinete: Optional[Dict[str, Any]] = None
    situacao: Optional[str] = None
    condicaoEleitoral: Optional[str] = None
    descricaoStatus: Optional[str] = None

class Deputado(BaseModel):
    id: int
    uri: str
    nomeCivil: str
    ultimoStatus: UltimoStatus
    cpf: Optional[str] = None
    sexo: Optional[str] = None
    urlWebsite: Optional[str] = None
    redeSocial: Optional[List[str]] = None
    dataNascimento: Optional[date] = None
    dataFalecimento: Optional[date] = None
    ufNascimento: Optional[str] = None
    municipioNascimento: Optional[str] = None

class DeputadoSimples(BaseModel):
    id: int
    uri: str
    nome: str
    siglaPartido: Optional[str] = None
    uriPartido: Optional[str] = None
    siglaUf: str
    idLegislatura: int
    urlFoto: str
    email: Optional[str] = None

class DeputadosResponse(BaseModel):
    dados: List[DeputadoSimples]

# Models for /deputados/{id}/despesas
class Despesa(BaseModel):
    ano: int
    mes: int
    tipoDespesa: str
    codDocumento: int
    tipoDocumento: str
    codTipoDocumento: int
    dataDocumento: date
    numDocumento: str
    valorDocumento: float
    urlDocumento: str
    nomeFornecedor: str
    cnpjCpfFornecedor: str
    valorLiquido: float
    valorGlosa: float
    numRessarcimento: Optional[str] = None
    codLote: Optional[int] = None
    parcela: Optional[int] = None

class DespesasResponse(BaseModel):
    dados: List[Despesa]

# Models for /deputados/{id}/discursos
class Discurso(BaseModel):
    dataHoraInicio: datetime
    dataHoraFim: Optional[datetime] = None
    faseEvento: Optional[Dict[str, Any]] = None
    tipoDiscurso: str
    urlTexto: Optional[str] = None
    urlAudio: Optional[str] = None
    urlVideo: Optional[str] = None
    keywords: Optional[str] = None
    sumario: Optional[str] = None
    transcricao: Optional[str] = None

class DiscursosResponse(BaseModel):
    dados: List[Discurso]

# Models for /votacoes
class Votacao(BaseModel):
    id: str
    uri: str
    data: date
    dataHoraRegistro: datetime
    siglaOrgao: str
    uriOrgao: str
    proposicaoObjeto: Optional[Dict[str, Any]] = None
    tipoVotacao: Dict[str, Any]
    ultimaApresentacaoProposicao: Optional[Dict[str, Any]] = None
    aprovacao: bool

class VotacoesResponse(BaseModel):
    dados: List[Votacao]

# Models for /votacoes/{id}/votos
class Voto(BaseModel):
    dataRegistroVoto: datetime
    tipoVoto: str
    deputado_: Dict[str, Any] = Field(..., alias="deputado")

class VotosResponse(BaseModel):
    dados: List[Voto]
    
# Response Model wrapper
class ApiResponse(BaseModel):
    dados: List[Any]
    links: List[Dict[str, str]]

