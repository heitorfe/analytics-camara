from sqlalchemy import Column, Integer, String, Float, Date, DateTime, ForeignKey, Boolean, Text, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from .database import Base

class Deputado(Base):
    __tablename__ = "deputados"

    id = Column(Integer, primary_key=True, index=True)
    uri = Column(String, nullable=False)
    nome_civil = Column(String, nullable=False)
    cpf = Column(String, nullable=True)
    sexo = Column(String, nullable=True)
    url_website = Column(String, nullable=True)
    data_nascimento = Column(Date, nullable=True)
    data_falecimento = Column(Date, nullable=True)
    uf_nascimento = Column(String, nullable=True)
    municipio_nascimento = Column(String, nullable=True)
    
    # UltimoStatus fields
    ultimo_status_id = Column(Integer, nullable=True)
    ultimo_status_nome = Column(String, nullable=True)
    ultimo_status_sigla_partido = Column(String, nullable=True)
    ultimo_status_uri_partido = Column(String, nullable=True)
    ultimo_status_sigla_uf = Column(String, nullable=True)
    ultimo_status_id_legislatura = Column(Integer, nullable=True)
    ultimo_status_url_foto = Column(String, nullable=True)
    ultimo_status_email = Column(String, nullable=True)
    ultimo_status_data = Column(Date, nullable=True)
    ultimo_status_nome_eleitoral = Column(String, nullable=True)
    ultimo_status_gabinete = Column(JSON, nullable=True)
    ultimo_status_situacao = Column(String, nullable=True)
    ultimo_status_condicao_eleitoral = Column(String, nullable=True)
    ultimo_status_descricao = Column(String, nullable=True)
    
    # Relationships
    despesas = relationship("Despesa", back_populates="deputado")
    discursos = relationship("Discurso", back_populates="deputado")
    votos = relationship("Voto", back_populates="deputado")

    def __repr__(self):
        return f"<Deputado(id={self.id}, nome='{self.nome_civil}')>"


class Despesa(Base):
    __tablename__ = "despesas"

    id = Column(Integer, primary_key=True, autoincrement=True)
    deputado_id = Column(Integer, ForeignKey("deputados.id"), nullable=False)
    ano = Column(Integer, nullable=False)
    mes = Column(Integer, nullable=False)
    tipo_despesa = Column(String, nullable=False)
    cod_documento = Column(Integer, nullable=False)
    tipo_documento = Column(String, nullable=False)
    cod_tipo_documento = Column(Integer, nullable=False)
    data_documento = Column(Date, nullable=False)
    num_documento = Column(String, nullable=False)
    valor_documento = Column(Float, nullable=False)
    url_documento = Column(String, nullable=False)
    nome_fornecedor = Column(String, nullable=False)
    cnpj_cpf_fornecedor = Column(String, nullable=False)
    valor_liquido = Column(Float, nullable=False)
    valor_glosa = Column(Float, nullable=False)
    num_ressarcimento = Column(String, nullable=True)
    cod_lote = Column(Integer, nullable=True)
    parcela = Column(Integer, nullable=True)
    
    # Relationship
    deputado = relationship("Deputado", back_populates="despesas")

    def __repr__(self):
        return f"<Despesa(id={self.id}, deputado_id={self.deputado_id}, valor={self.valor_documento})>"


class Discurso(Base):
    __tablename__ = "discursos"

    id = Column(Integer, primary_key=True, autoincrement=True)
    deputado_id = Column(Integer, ForeignKey("deputados.id"), nullable=False)
    data_hora_inicio = Column(DateTime, nullable=False)
    data_hora_fim = Column(DateTime, nullable=True)
    fase_evento = Column(JSON, nullable=True)
    tipo_discurso = Column(String, nullable=False)
    url_texto = Column(String, nullable=True)
    url_audio = Column(String, nullable=True)
    url_video = Column(String, nullable=True)
    keywords = Column(String, nullable=True)
    sumario = Column(Text, nullable=True)
    transcricao = Column(Text, nullable=True)
    
    # Relationship
    deputado = relationship("Deputado", back_populates="discursos")

    def __repr__(self):
        return f"<Discurso(id={self.id}, deputado_id={self.deputado_id}, data={self.data_hora_inicio})>"


class Votacao(Base):
    __tablename__ = "votacoes"

    id = Column(String, primary_key=True, index=True)
    uri = Column(String, nullable=False)
    data = Column(Date, nullable=False)
    data_hora_registro = Column(DateTime, nullable=False)
    sigla_orgao = Column(String, nullable=False)
    uri_orgao = Column(String, nullable=False)
    proposicao_objeto = Column(JSON, nullable=True)
    tipo_votacao = Column(JSON, nullable=False)
    ultima_apresentacao_proposicao = Column(JSON, nullable=True)
    aprovacao = Column(Boolean, nullable=False)
    
    # Relationship
    votos = relationship("Voto", back_populates="votacao")

    def __repr__(self):
        return f"<Votacao(id='{self.id}', data='{self.data}', aprovacao={self.aprovacao})>"


class Voto(Base):
    __tablename__ = "votos"

    id = Column(Integer, primary_key=True, autoincrement=True)
    votacao_id = Column(String, ForeignKey("votacoes.id"), nullable=False)
    deputado_id = Column(Integer, ForeignKey("deputados.id"), nullable=False)
    data_registro_voto = Column(DateTime, nullable=False)
    tipo_voto = Column(String, nullable=False)
    
    # Relationships
    votacao = relationship("Votacao", back_populates="votos")
    deputado = relationship("Deputado", back_populates="votos")

    def __repr__(self):
        return f"<Voto(votacao_id='{self.votacao_id}', deputado_id={self.deputado_id}, tipo='{self.tipo_voto}')>"