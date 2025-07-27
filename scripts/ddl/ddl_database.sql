--
-- PostgreSQL database dump
--

-- Dumped from database version 16.9 (Ubuntu 16.9-0ubuntu0.24.04.1)
-- Dumped by pg_dump version 16.9 (Ubuntu 16.9-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: dim_eixo; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.dim_eixo (
    sk_eixo integer NOT NULL,
    nk_eixo character varying(255) NOT NULL,
    descricao character varying(255) NOT NULL
);


ALTER TABLE public.dim_eixo OWNER TO devops;

--
-- Name: dim_eixos_sk_eixos_seq; Type: SEQUENCE; Schema: public; Owner: devops
--

CREATE SEQUENCE public.dim_eixos_sk_eixos_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_eixos_sk_eixos_seq OWNER TO devops;

--
-- Name: dim_eixos_sk_eixos_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: devops
--

ALTER SEQUENCE public.dim_eixos_sk_eixos_seq OWNED BY public.dim_eixo.sk_eixo;


--
-- Name: dim_especie; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.dim_especie (
    sk_especie integer NOT NULL,
    nk_especie character varying(255) NOT NULL,
    descricao character varying(255) NOT NULL
);


ALTER TABLE public.dim_especie OWNER TO devops;

--
-- Name: dim_especie_sk_especie_seq; Type: SEQUENCE; Schema: public; Owner: devops
--

CREATE SEQUENCE public.dim_especie_sk_especie_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_especie_sk_especie_seq OWNER TO devops;

--
-- Name: dim_especie_sk_especie_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: devops
--

ALTER SEQUENCE public.dim_especie_sk_especie_seq OWNED BY public.dim_especie.sk_especie;


--
-- Name: dim_executor; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.dim_executor (
    sk_executor integer NOT NULL,
    nk_executor character varying(255) NOT NULL,
    descricao character varying(255) NOT NULL
);


ALTER TABLE public.dim_executor OWNER TO devops;

--
-- Name: dim_executores_sk_executor_seq; Type: SEQUENCE; Schema: public; Owner: devops
--

CREATE SEQUENCE public.dim_executores_sk_executor_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_executores_sk_executor_seq OWNER TO devops;

--
-- Name: dim_executores_sk_executor_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: devops
--

ALTER SEQUENCE public.dim_executores_sk_executor_seq OWNED BY public.dim_executor.sk_executor;


--
-- Name: dim_fonte_recurso; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.dim_fonte_recurso (
    sk_fonte_recurso integer NOT NULL,
    nk_fonte_recurso character varying(255) NOT NULL,
    descricao character varying(255) NOT NULL
);


ALTER TABLE public.dim_fonte_recurso OWNER TO devops;

--
-- Name: dim_fonte_recurso_sk_fonte_recurso_seq; Type: SEQUENCE; Schema: public; Owner: devops
--

CREATE SEQUENCE public.dim_fonte_recurso_sk_fonte_recurso_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_fonte_recurso_sk_fonte_recurso_seq OWNER TO devops;

--
-- Name: dim_fonte_recurso_sk_fonte_recurso_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: devops
--

ALTER SEQUENCE public.dim_fonte_recurso_sk_fonte_recurso_seq OWNED BY public.dim_fonte_recurso.sk_fonte_recurso;


--
-- Name: dim_natureza; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.dim_natureza (
    sk_natureza integer NOT NULL,
    nk_natureza character varying(255) NOT NULL,
    descricao character varying(255) NOT NULL
);


ALTER TABLE public.dim_natureza OWNER TO devops;

--
-- Name: dim_natureza_sk_natureza_seq; Type: SEQUENCE; Schema: public; Owner: devops
--

CREATE SEQUENCE public.dim_natureza_sk_natureza_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_natureza_sk_natureza_seq OWNER TO devops;

--
-- Name: dim_natureza_sk_natureza_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: devops
--

ALTER SEQUENCE public.dim_natureza_sk_natureza_seq OWNED BY public.dim_natureza.sk_natureza;


--
-- Name: dim_projeto_investimento; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.dim_projeto_investimento (
    sk_projeto_investimento integer NOT NULL,
    nk_projeto_investimento character varying(100) NOT NULL,
    data_projeto date NOT NULL,
    nome_projeto character varying,
    cep character varying(50),
    endereco character varying,
    descricao_projeto character varying,
    funcao_social character varying,
    meta_global character varying,
    data_inicial_prevista date,
    data_final_prevista date,
    data_inicial_efetiva date,
    data_final_efetiva date,
    especie character varying(50),
    natureza character varying(255),
    natureza_outras character varying,
    situacao character varying(50)
);


ALTER TABLE public.dim_projeto_investimento OWNER TO devops;

--
-- Name: dim_projeto_investimento_sk_projeto_investimento_seq; Type: SEQUENCE; Schema: public; Owner: devops
--

CREATE SEQUENCE public.dim_projeto_investimento_sk_projeto_investimento_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_projeto_investimento_sk_projeto_investimento_seq OWNER TO devops;

--
-- Name: dim_projeto_investimento_sk_projeto_investimento_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: devops
--

ALTER SEQUENCE public.dim_projeto_investimento_sk_projeto_investimento_seq OWNED BY public.dim_projeto_investimento.sk_projeto_investimento;


--
-- Name: dim_repassador; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.dim_repassador (
    sk_repassador integer NOT NULL,
    nk_repassador character varying(255) NOT NULL,
    descricao character varying(255) NOT NULL
);


ALTER TABLE public.dim_repassador OWNER TO devops;

--
-- Name: dim_repassador_sk_repassador_seq; Type: SEQUENCE; Schema: public; Owner: devops
--

CREATE SEQUENCE public.dim_repassador_sk_repassador_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_repassador_sk_repassador_seq OWNER TO devops;

--
-- Name: dim_repassador_sk_repassador_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: devops
--

ALTER SEQUENCE public.dim_repassador_sk_repassador_seq OWNED BY public.dim_repassador.sk_repassador;


--
-- Name: dim_situacao; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.dim_situacao (
    sk_situacao integer NOT NULL,
    nk_situacao character varying(255) NOT NULL,
    descricao character varying(255) NOT NULL
);


ALTER TABLE public.dim_situacao OWNER TO devops;

--
-- Name: dim_situacao_sk_situacao_seq; Type: SEQUENCE; Schema: public; Owner: devops
--

CREATE SEQUENCE public.dim_situacao_sk_situacao_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_situacao_sk_situacao_seq OWNER TO devops;

--
-- Name: dim_situacao_sk_situacao_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: devops
--

ALTER SEQUENCE public.dim_situacao_sk_situacao_seq OWNED BY public.dim_situacao.sk_situacao;


--
-- Name: dim_sub_tipo; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.dim_sub_tipo (
    sk_sub_tipo integer NOT NULL,
    nk_sub_tipo character varying(255) NOT NULL,
    descricao character varying(255) NOT NULL
);


ALTER TABLE public.dim_sub_tipo OWNER TO devops;

--
-- Name: dim_sub_tipo_sk_sub_tipo_seq; Type: SEQUENCE; Schema: public; Owner: devops
--

CREATE SEQUENCE public.dim_sub_tipo_sk_sub_tipo_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_sub_tipo_sk_sub_tipo_seq OWNER TO devops;

--
-- Name: dim_sub_tipo_sk_sub_tipo_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: devops
--

ALTER SEQUENCE public.dim_sub_tipo_sk_sub_tipo_seq OWNED BY public.dim_sub_tipo.sk_sub_tipo;


--
-- Name: dim_tipo; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.dim_tipo (
    sk_tipo integer NOT NULL,
    nk_tipo character varying(255) NOT NULL,
    descricao character varying(255) NOT NULL
);


ALTER TABLE public.dim_tipo OWNER TO devops;

--
-- Name: dim_tipo_sk_tipo_seq; Type: SEQUENCE; Schema: public; Owner: devops
--

CREATE SEQUENCE public.dim_tipo_sk_tipo_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_tipo_sk_tipo_seq OWNER TO devops;

--
-- Name: dim_tipo_sk_tipo_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: devops
--

ALTER SEQUENCE public.dim_tipo_sk_tipo_seq OWNED BY public.dim_tipo.sk_tipo;


--
-- Name: dim_tomador; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.dim_tomador (
    sk_tomador integer NOT NULL,
    nk_tomador character varying(255) NOT NULL,
    descricao character varying(255) NOT NULL
);


ALTER TABLE public.dim_tomador OWNER TO devops;

--
-- Name: dim_tomador_sk_tomador_seq; Type: SEQUENCE; Schema: public; Owner: devops
--

CREATE SEQUENCE public.dim_tomador_sk_tomador_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_tomador_sk_tomador_seq OWNER TO devops;

--
-- Name: dim_tomador_sk_tomador_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: devops
--

ALTER SEQUENCE public.dim_tomador_sk_tomador_seq OWNED BY public.dim_tomador.sk_tomador;


--
-- Name: dim_uf; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.dim_uf (
    sk_uf integer NOT NULL,
    nk_uf character varying(255) NOT NULL,
    descricao character varying(255) NOT NULL
);


ALTER TABLE public.dim_uf OWNER TO devops;

--
-- Name: dim_uf_sk_uf_seq; Type: SEQUENCE; Schema: public; Owner: devops
--

CREATE SEQUENCE public.dim_uf_sk_uf_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_uf_sk_uf_seq OWNER TO devops;

--
-- Name: dim_uf_sk_uf_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: devops
--

ALTER SEQUENCE public.dim_uf_sk_uf_seq OWNED BY public.dim_uf.sk_uf;


--
-- Name: fact_projeto_investimento; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.fact_projeto_investimento (
    sk_projeto_investimento integer NOT NULL,
    prazo_previsto integer,
    prazo_efetivo integer,
    qtd_empregos_gerados integer,
    pop_beneficiada integer,
    valor_investimento_previsto double precision,
    valor_execucao double precision
);


ALTER TABLE public.fact_projeto_investimento OWNER TO devops;

--
-- Name: fact_projeto_investimento_eixos; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.fact_projeto_investimento_eixos (
    sk_projeto_investimento integer NOT NULL,
    sk_eixo integer NOT NULL
);


ALTER TABLE public.fact_projeto_investimento_eixos OWNER TO devops;

--
-- Name: fact_projeto_investimento_executores; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.fact_projeto_investimento_executores (
    sk_projeto_investimento integer NOT NULL,
    sk_executor integer NOT NULL
);


ALTER TABLE public.fact_projeto_investimento_executores OWNER TO devops;

--
-- Name: fact_projeto_investimento_fonte_recurso; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.fact_projeto_investimento_fonte_recurso (
    sk_projeto_investimento integer NOT NULL,
    sk_fonte_recurso integer NOT NULL,
    valor_investimento_previsto double precision
);


ALTER TABLE public.fact_projeto_investimento_fonte_recurso OWNER TO devops;

--
-- Name: fact_projeto_investimento_repassadores; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.fact_projeto_investimento_repassadores (
    sk_projeto_investimento integer NOT NULL,
    sk_repassador integer NOT NULL
);


ALTER TABLE public.fact_projeto_investimento_repassadores OWNER TO devops;

--
-- Name: fact_projeto_investimento_sub_tipos; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.fact_projeto_investimento_sub_tipos (
    sk_projeto_investimento integer NOT NULL,
    sk_sub_tipo integer NOT NULL
);


ALTER TABLE public.fact_projeto_investimento_sub_tipos OWNER TO devops;

--
-- Name: fact_projeto_investimento_tipos; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.fact_projeto_investimento_tipos (
    sk_projeto_investimento integer NOT NULL,
    sk_tipo integer NOT NULL
);


ALTER TABLE public.fact_projeto_investimento_tipos OWNER TO devops;

--
-- Name: fact_projeto_investimento_tomadores; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.fact_projeto_investimento_tomadores (
    sk_projeto_investimento integer NOT NULL,
    sk_tomador integer NOT NULL
);


ALTER TABLE public.fact_projeto_investimento_tomadores OWNER TO devops;

--
-- Name: stg_execucao_financeira; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.stg_execucao_financeira (
    "autorEmenda" text,
    "codigoAmparoLegal" bigint,
    "descricaoEmpenho" text,
    "fonteRecurso" text,
    "idProjetoInvestimento" text,
    "informacoesComplementares" text,
    "localEntrega" text,
    "naturezaDespesa" text,
    "nomeEsferaOrcamentaria" text,
    "nomeFavorecido" text,
    "nomeTipoEmpenho" text,
    "nrPtres" text,
    "numeroNotaEmpenhoGerada" text,
    "numeroProcesso" text,
    pagina bigint,
    "planoInterno" text,
    "planoOrcamentario" text,
    "resultadoPrimario" text,
    "tamanhoDaPagina" bigint,
    "tipoCredito" text,
    "ugEmitente" text,
    "ugResponsavel" bigint,
    "unidadeOrcamentaria" text,
    "valorEmpenho" double precision,
    "nomeArquivo" text NOT NULL
);


ALTER TABLE public.stg_execucao_financeira OWNER TO devops;

--
-- Name: stg_execucao_financeira_teste; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.stg_execucao_financeira_teste (
    "autorEmenda" text,
    "codigoAmparoLegal" bigint,
    "descricaoEmpenho" text,
    "fonteRecurso" text,
    "idProjetoInvestimento" text,
    "informacoesComplementares" text,
    "localEntrega" text,
    "naturezaDespesa" text,
    "nomeEsferaOrcamentaria" text,
    "nomeFavorecido" text,
    "nomeTipoEmpenho" text,
    "nrPtres" text,
    "numeroNotaEmpenhoGerada" text,
    "numeroProcesso" text,
    pagina bigint,
    "planoInterno" text,
    "planoOrcamentario" text,
    "resultadoPrimario" text,
    "tamanhoDaPagina" bigint,
    "tipoCredito" text,
    "ugEmitente" text,
    "ugResponsavel" bigint,
    "unidadeOrcamentaria" text,
    "valorEmpenho" double precision,
    "nomeArquivo" text NOT NULL
);


ALTER TABLE public.stg_execucao_financeira_teste OWNER TO devops;

--
-- Name: stg_projeto_investimento; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.stg_projeto_investimento (
    idunico character varying NOT NULL,
    datacadastro date NOT NULL,
    nome character varying,
    cep character varying,
    endereco character varying,
    descricao character varying,
    funcaosocial character varying,
    metaglobal character varying,
    datainicialprevista date,
    datafinalprevista date,
    datainicialefetiva date,
    datafinalefetiva date,
    especie character varying,
    natureza character varying,
    naturezaoutras character varying,
    situacao character varying,
    descplanonacionalpoliticavinculado character varying,
    uf character varying,
    qdtempregosgerados character varying,
    descpopulacaobeneficiada character varying,
    populacaobeneficiada character varying,
    observacoespertinentes character varying,
    ismodeladaporbim boolean,
    datasituacao date,
    nomearquivo character varying
);


ALTER TABLE public.stg_projeto_investimento OWNER TO devops;

--
-- Name: stg_projeto_investimento_eixos; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.stg_projeto_investimento_eixos (
    idunico character varying NOT NULL,
    descricao character varying NOT NULL,
    id bigint NOT NULL,
    datacadastro date NOT NULL
);


ALTER TABLE public.stg_projeto_investimento_eixos OWNER TO devops;

--
-- Name: stg_projeto_investimento_executores; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.stg_projeto_investimento_executores (
    idunico character varying NOT NULL,
    nome character varying NOT NULL,
    codigo bigint NOT NULL,
    datacadastro date NOT NULL
);


ALTER TABLE public.stg_projeto_investimento_executores OWNER TO devops;

--
-- Name: stg_projeto_investimento_fontes_de_recurso; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.stg_projeto_investimento_fontes_de_recurso (
    idunico character varying NOT NULL,
    origem character varying NOT NULL,
    datacadastro date NOT NULL,
    valorinvestimentoprevisto double precision
);


ALTER TABLE public.stg_projeto_investimento_fontes_de_recurso OWNER TO devops;

--
-- Name: stg_projeto_investimento_geometria; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.stg_projeto_investimento_geometria (
    idunico character varying NOT NULL,
    datacadastro date NOT NULL,
    cepareaexecutora character varying,
    datacriacao date,
    datametadado date,
    datum character varying,
    enderecoareaexecutora character varying,
    geometria character varying,
    infoadicionais character varying,
    nomeareaexecutora character varying,
    origem character varying,
    paisareaexecutora character varying
);


ALTER TABLE public.stg_projeto_investimento_geometria OWNER TO devops;

--
-- Name: stg_projeto_investimento_repassadores; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.stg_projeto_investimento_repassadores (
    idunico character varying NOT NULL,
    nome character varying NOT NULL,
    codigo bigint NOT NULL,
    datacadastro date NOT NULL
);


ALTER TABLE public.stg_projeto_investimento_repassadores OWNER TO devops;

--
-- Name: stg_projeto_investimento_sub_tipos; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.stg_projeto_investimento_sub_tipos (
    idunico character varying NOT NULL,
    descricao character varying NOT NULL,
    id bigint NOT NULL,
    idtipo bigint NOT NULL,
    datacadastro date NOT NULL
);


ALTER TABLE public.stg_projeto_investimento_sub_tipos OWNER TO devops;

--
-- Name: stg_projeto_investimento_teste; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.stg_projeto_investimento_teste (
    idunico character varying NOT NULL,
    datacadastro date NOT NULL,
    nome character varying,
    cep character varying,
    endereco character varying,
    descricao character varying,
    funcaosocial character varying,
    metaglobal character varying,
    datainicialprevista date,
    datafinalprevista date,
    datainicialefetiva date,
    datafinalefetiva date,
    especie character varying,
    natureza character varying,
    naturezaoutras character varying,
    situacao character varying,
    descplanonacionalpoliticavinculado character varying,
    uf character varying,
    qdtempregosgerados character varying,
    descpopulacaobeneficiada character varying,
    populacaobeneficiada character varying,
    observacoespertinentes character varying,
    ismodeladaporbim boolean,
    datasituacao date,
    nomearquivo character varying
);


ALTER TABLE public.stg_projeto_investimento_teste OWNER TO devops;

--
-- Name: stg_projeto_investimento_tipos; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.stg_projeto_investimento_tipos (
    idunico character varying(255) NOT NULL,
    descricao character varying(255) NOT NULL,
    id bigint NOT NULL,
    ideixo bigint NOT NULL,
    datacadastro date NOT NULL
);


ALTER TABLE public.stg_projeto_investimento_tipos OWNER TO devops;

--
-- Name: stg_projeto_investimento_tomadores; Type: TABLE; Schema: public; Owner: devops
--

CREATE TABLE public.stg_projeto_investimento_tomadores (
    idunico character varying NOT NULL,
    nome character varying NOT NULL,
    codigo bigint NOT NULL,
    datacadastro date NOT NULL
);


ALTER TABLE public.stg_projeto_investimento_tomadores OWNER TO devops;

--
-- Name: dim_eixo sk_eixo; Type: DEFAULT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_eixo ALTER COLUMN sk_eixo SET DEFAULT nextval('public.dim_eixos_sk_eixos_seq'::regclass);


--
-- Name: dim_especie sk_especie; Type: DEFAULT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_especie ALTER COLUMN sk_especie SET DEFAULT nextval('public.dim_especie_sk_especie_seq'::regclass);


--
-- Name: dim_executor sk_executor; Type: DEFAULT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_executor ALTER COLUMN sk_executor SET DEFAULT nextval('public.dim_executores_sk_executor_seq'::regclass);


--
-- Name: dim_fonte_recurso sk_fonte_recurso; Type: DEFAULT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_fonte_recurso ALTER COLUMN sk_fonte_recurso SET DEFAULT nextval('public.dim_fonte_recurso_sk_fonte_recurso_seq'::regclass);


--
-- Name: dim_natureza sk_natureza; Type: DEFAULT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_natureza ALTER COLUMN sk_natureza SET DEFAULT nextval('public.dim_natureza_sk_natureza_seq'::regclass);


--
-- Name: dim_projeto_investimento sk_projeto_investimento; Type: DEFAULT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_projeto_investimento ALTER COLUMN sk_projeto_investimento SET DEFAULT nextval('public.dim_projeto_investimento_sk_projeto_investimento_seq'::regclass);


--
-- Name: dim_repassador sk_repassador; Type: DEFAULT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_repassador ALTER COLUMN sk_repassador SET DEFAULT nextval('public.dim_repassador_sk_repassador_seq'::regclass);


--
-- Name: dim_situacao sk_situacao; Type: DEFAULT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_situacao ALTER COLUMN sk_situacao SET DEFAULT nextval('public.dim_situacao_sk_situacao_seq'::regclass);


--
-- Name: dim_sub_tipo sk_sub_tipo; Type: DEFAULT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_sub_tipo ALTER COLUMN sk_sub_tipo SET DEFAULT nextval('public.dim_sub_tipo_sk_sub_tipo_seq'::regclass);


--
-- Name: dim_tipo sk_tipo; Type: DEFAULT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_tipo ALTER COLUMN sk_tipo SET DEFAULT nextval('public.dim_tipo_sk_tipo_seq'::regclass);


--
-- Name: dim_tomador sk_tomador; Type: DEFAULT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_tomador ALTER COLUMN sk_tomador SET DEFAULT nextval('public.dim_tomador_sk_tomador_seq'::regclass);


--
-- Name: dim_uf sk_uf; Type: DEFAULT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_uf ALTER COLUMN sk_uf SET DEFAULT nextval('public.dim_uf_sk_uf_seq'::regclass);


--
-- Name: dim_eixo dim_eixo_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_eixo
    ADD CONSTRAINT dim_eixo_pk PRIMARY KEY (sk_eixo);


--
-- Name: dim_especie dim_especie_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_especie
    ADD CONSTRAINT dim_especie_pk PRIMARY KEY (sk_especie);


--
-- Name: dim_executor dim_executores_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_executor
    ADD CONSTRAINT dim_executores_pk PRIMARY KEY (sk_executor);


--
-- Name: dim_fonte_recurso dim_fonte_recurso_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_fonte_recurso
    ADD CONSTRAINT dim_fonte_recurso_pk PRIMARY KEY (sk_fonte_recurso);


--
-- Name: dim_natureza dim_natureza_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_natureza
    ADD CONSTRAINT dim_natureza_pk PRIMARY KEY (sk_natureza);


--
-- Name: dim_projeto_investimento dim_projeto_investimento_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_projeto_investimento
    ADD CONSTRAINT dim_projeto_investimento_pk PRIMARY KEY (sk_projeto_investimento);


--
-- Name: dim_repassador dim_repassador_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_repassador
    ADD CONSTRAINT dim_repassador_pk PRIMARY KEY (sk_repassador);


--
-- Name: dim_situacao dim_situacao_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_situacao
    ADD CONSTRAINT dim_situacao_pk PRIMARY KEY (sk_situacao);


--
-- Name: dim_sub_tipo dim_sub_tipo_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_sub_tipo
    ADD CONSTRAINT dim_sub_tipo_pk PRIMARY KEY (sk_sub_tipo);


--
-- Name: dim_tipo dim_tipo_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_tipo
    ADD CONSTRAINT dim_tipo_pk PRIMARY KEY (sk_tipo);


--
-- Name: dim_tomador dim_tomador_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_tomador
    ADD CONSTRAINT dim_tomador_pk PRIMARY KEY (sk_tomador);


--
-- Name: dim_uf dim_uf_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.dim_uf
    ADD CONSTRAINT dim_uf_pk PRIMARY KEY (sk_uf);


--
-- Name: fact_projeto_investimento_eixos fact_projeto_investimento_eixos_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_eixos
    ADD CONSTRAINT fact_projeto_investimento_eixos_pk PRIMARY KEY (sk_projeto_investimento, sk_eixo);


--
-- Name: fact_projeto_investimento_executores fact_projeto_investimento_executores_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_executores
    ADD CONSTRAINT fact_projeto_investimento_executores_pk PRIMARY KEY (sk_projeto_investimento, sk_executor);


--
-- Name: fact_projeto_investimento_fonte_recurso fact_projeto_investimento_fonte_recurso_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_fonte_recurso
    ADD CONSTRAINT fact_projeto_investimento_fonte_recurso_pk PRIMARY KEY (sk_projeto_investimento, sk_fonte_recurso);


--
-- Name: fact_projeto_investimento fact_projeto_investimento_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento
    ADD CONSTRAINT fact_projeto_investimento_pk PRIMARY KEY (sk_projeto_investimento);


--
-- Name: fact_projeto_investimento_repassadores fact_projeto_investimento_repassadores_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_repassadores
    ADD CONSTRAINT fact_projeto_investimento_repassadores_pk PRIMARY KEY (sk_projeto_investimento, sk_repassador);


--
-- Name: fact_projeto_investimento_sub_tipos fact_projeto_investimento_sub_tipos_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_sub_tipos
    ADD CONSTRAINT fact_projeto_investimento_sub_tipos_pk PRIMARY KEY (sk_projeto_investimento, sk_sub_tipo);


--
-- Name: fact_projeto_investimento_tipos fact_projeto_investimento_tipos_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_tipos
    ADD CONSTRAINT fact_projeto_investimento_tipos_pk PRIMARY KEY (sk_projeto_investimento, sk_tipo);


--
-- Name: fact_projeto_investimento_tomadores fact_projeto_investimento_tomadores_pk; Type: CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_tomadores
    ADD CONSTRAINT fact_projeto_investimento_tomadores_pk PRIMARY KEY (sk_projeto_investimento, sk_tomador);


--
-- Name: dim_especie_nk_especie_idx; Type: INDEX; Schema: public; Owner: devops
--

CREATE INDEX dim_especie_nk_especie_idx ON public.dim_especie USING btree (nk_especie);


--
-- Name: dim_executores_nk_executor_idx; Type: INDEX; Schema: public; Owner: devops
--

CREATE INDEX dim_executores_nk_executor_idx ON public.dim_executor USING btree (nk_executor);


--
-- Name: dim_fonte_recurso_nk_fonte_recurso_idx; Type: INDEX; Schema: public; Owner: devops
--

CREATE INDEX dim_fonte_recurso_nk_fonte_recurso_idx ON public.dim_fonte_recurso USING btree (nk_fonte_recurso);


--
-- Name: dim_natureza_nk_natureza_idx; Type: INDEX; Schema: public; Owner: devops
--

CREATE INDEX dim_natureza_nk_natureza_idx ON public.dim_natureza USING btree (nk_natureza);


--
-- Name: dim_projeto_investimento_nk_projeto_investimento_idx; Type: INDEX; Schema: public; Owner: devops
--

CREATE INDEX dim_projeto_investimento_nk_projeto_investimento_idx ON public.dim_projeto_investimento USING btree (nk_projeto_investimento);


--
-- Name: dim_repassador_nk_repassador_idx; Type: INDEX; Schema: public; Owner: devops
--

CREATE INDEX dim_repassador_nk_repassador_idx ON public.dim_repassador USING btree (nk_repassador);


--
-- Name: dim_situacao_nk_situacao_idx; Type: INDEX; Schema: public; Owner: devops
--

CREATE INDEX dim_situacao_nk_situacao_idx ON public.dim_situacao USING btree (nk_situacao);


--
-- Name: dim_sub_tipo_nk_sub_tipo_idx; Type: INDEX; Schema: public; Owner: devops
--

CREATE INDEX dim_sub_tipo_nk_sub_tipo_idx ON public.dim_sub_tipo USING btree (nk_sub_tipo);


--
-- Name: dim_tipo_nk_tipo_idx; Type: INDEX; Schema: public; Owner: devops
--

CREATE INDEX dim_tipo_nk_tipo_idx ON public.dim_tipo USING btree (nk_tipo);


--
-- Name: dim_tomador_nk_tomador_idx; Type: INDEX; Schema: public; Owner: devops
--

CREATE INDEX dim_tomador_nk_tomador_idx ON public.dim_tomador USING btree (nk_tomador);


--
-- Name: dim_uf_nk_uf_idx; Type: INDEX; Schema: public; Owner: devops
--

CREATE INDEX dim_uf_nk_uf_idx ON public.dim_uf USING btree (nk_uf);


--
-- Name: stg_projeto_investimento_teste_idunico_idx; Type: INDEX; Schema: public; Owner: devops
--

CREATE INDEX stg_projeto_investimento_teste_idunico_idx ON public.stg_projeto_investimento_teste USING btree (idunico);


--
-- Name: fact_projeto_investimento fact_projeto_investimento_dim_projeto_investimento_fk; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento
    ADD CONSTRAINT fact_projeto_investimento_dim_projeto_investimento_fk FOREIGN KEY (sk_projeto_investimento) REFERENCES public.dim_projeto_investimento(sk_projeto_investimento);


--
-- Name: fact_projeto_investimento_eixos fact_projeto_investimento_eixos_dim_eixo_fk; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_eixos
    ADD CONSTRAINT fact_projeto_investimento_eixos_dim_eixo_fk FOREIGN KEY (sk_eixo) REFERENCES public.dim_eixo(sk_eixo);


--
-- Name: fact_projeto_investimento_eixos fact_projeto_investimento_eixos_dim_projeto_investimento_fk; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_eixos
    ADD CONSTRAINT fact_projeto_investimento_eixos_dim_projeto_investimento_fk FOREIGN KEY (sk_projeto_investimento) REFERENCES public.dim_projeto_investimento(sk_projeto_investimento);


--
-- Name: fact_projeto_investimento_executores fact_projeto_investimento_executores_dim_executor_fk; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_executores
    ADD CONSTRAINT fact_projeto_investimento_executores_dim_executor_fk FOREIGN KEY (sk_executor) REFERENCES public.dim_executor(sk_executor);


--
-- Name: fact_projeto_investimento_executores fact_projeto_investimento_executores_dim_projeto_investimento_f; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_executores
    ADD CONSTRAINT fact_projeto_investimento_executores_dim_projeto_investimento_f FOREIGN KEY (sk_projeto_investimento) REFERENCES public.dim_projeto_investimento(sk_projeto_investimento);


--
-- Name: fact_projeto_investimento_fonte_recurso fact_projeto_investimento_fonte_recurso_dim_fonte_recurso_fk; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_fonte_recurso
    ADD CONSTRAINT fact_projeto_investimento_fonte_recurso_dim_fonte_recurso_fk FOREIGN KEY (sk_fonte_recurso) REFERENCES public.dim_fonte_recurso(sk_fonte_recurso);


--
-- Name: fact_projeto_investimento_fonte_recurso fact_projeto_investimento_fonte_recurso_dim_projeto_investiment; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_fonte_recurso
    ADD CONSTRAINT fact_projeto_investimento_fonte_recurso_dim_projeto_investiment FOREIGN KEY (sk_projeto_investimento) REFERENCES public.dim_projeto_investimento(sk_projeto_investimento);


--
-- Name: fact_projeto_investimento_repassadores fact_projeto_investimento_repassadores_dim_projeto_investimento; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_repassadores
    ADD CONSTRAINT fact_projeto_investimento_repassadores_dim_projeto_investimento FOREIGN KEY (sk_projeto_investimento) REFERENCES public.dim_projeto_investimento(sk_projeto_investimento);


--
-- Name: fact_projeto_investimento_repassadores fact_projeto_investimento_repassadores_dim_repassador_fk; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_repassadores
    ADD CONSTRAINT fact_projeto_investimento_repassadores_dim_repassador_fk FOREIGN KEY (sk_repassador) REFERENCES public.dim_repassador(sk_repassador);


--
-- Name: fact_projeto_investimento_sub_tipos fact_projeto_investimento_sub_tipos_dim_projeto_investimento_fk; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_sub_tipos
    ADD CONSTRAINT fact_projeto_investimento_sub_tipos_dim_projeto_investimento_fk FOREIGN KEY (sk_projeto_investimento) REFERENCES public.dim_projeto_investimento(sk_projeto_investimento);


--
-- Name: fact_projeto_investimento_sub_tipos fact_projeto_investimento_sub_tipos_dim_sub_tipo_fk; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_sub_tipos
    ADD CONSTRAINT fact_projeto_investimento_sub_tipos_dim_sub_tipo_fk FOREIGN KEY (sk_sub_tipo) REFERENCES public.dim_sub_tipo(sk_sub_tipo);


--
-- Name: fact_projeto_investimento_tipos fact_projeto_investimento_tipos_dim_projeto_investimento_fk; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_tipos
    ADD CONSTRAINT fact_projeto_investimento_tipos_dim_projeto_investimento_fk FOREIGN KEY (sk_projeto_investimento) REFERENCES public.dim_projeto_investimento(sk_projeto_investimento);


--
-- Name: fact_projeto_investimento_tipos fact_projeto_investimento_tipos_dim_tipo_fk; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_tipos
    ADD CONSTRAINT fact_projeto_investimento_tipos_dim_tipo_fk FOREIGN KEY (sk_tipo) REFERENCES public.dim_tipo(sk_tipo);


--
-- Name: fact_projeto_investimento_tomadores fact_projeto_investimento_tomadores_dim_projeto_investimento_fk; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_tomadores
    ADD CONSTRAINT fact_projeto_investimento_tomadores_dim_projeto_investimento_fk FOREIGN KEY (sk_projeto_investimento) REFERENCES public.dim_projeto_investimento(sk_projeto_investimento);


--
-- Name: fact_projeto_investimento_tomadores fact_projeto_investimento_tomadores_dim_tomador_fk; Type: FK CONSTRAINT; Schema: public; Owner: devops
--

ALTER TABLE ONLY public.fact_projeto_investimento_tomadores
    ADD CONSTRAINT fact_projeto_investimento_tomadores_dim_tomador_fk FOREIGN KEY (sk_tomador) REFERENCES public.dim_tomador(sk_tomador);


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: pg_database_owner
--

GRANT ALL ON SCHEMA public TO devops;


--
-- PostgreSQL database dump complete
--

