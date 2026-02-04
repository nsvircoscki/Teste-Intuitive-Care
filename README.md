# Teste Técnico Intuitive Care - Nícolas
Este projeto consiste em uma solução Full Stack para o processamento, armazenamento, exposição e visualização de dados financeiros de operadoras de saúde.

A solução abrange:

ETL: Extração de dados da ANS, limpeza, tratamento de erros e enriquecimento.

Banco de Dados: Modelagem relacional e persistência em SQL.

API REST: Backend desenvolvido em Python para consulta de dados.

Frontend: Dashboard interativo para visualização de estatísticas.

Pré-requisitos
Python 3.9 ou superior

Navegador Web moderno

Instalação e Configuração
Recomenda-se o uso de um ambiente virtual (venv) para isolar as dependências.

Crie e ative o ambiente virtual:


python -m venv venv
.\venv\Scripts\activate
Instale as dependências:

pip install -r requirements.txt
Execução do Projeto
Siga a ordem abaixo para garantir que os dados sejam processados corretamente antes de iniciar a aplicação.

1. (ETL)
Devido à instabilidade observada nos servidores da ANS durante o desenvolvimento, foi implementado um mecanismo de fallback (Mock) para dados cadastrais.

Execute os scripts na seguinte ordem:


# Gera dados cadastrais locais (Fallback)
python src/gerar_mock.py

# Tenta baixar e extrair os arquivos de despesas (Demonstrações Contábeis)
python src/main.py

# Consolida, limpa e cruza os dados de despesas com o cadastro
python src/transformacao.py

2. Banco de Dados
Cria o banco de dados SQLite (intuitive_care.db) e tabelas, e preenche com os dados processados (CSV).

python src/banco_de_dados.py

3. (API)
Inicie o servidor da API. O terminal deve permanecer aberto.


uvicorn src.api:app --reload
A documentação interativa (Swagger UI) estará disponível em: http://127.0.0.1:8000/docs

4. Frontend
Em um novo terminal, inicie um servidor HTTP simples para servir o arquivo estático do frontend.


cd frontend
python -m http.server 8080
Acesse o dashboard em: http://localhost:8080

Decisões Técnicas e Trade-offs
Tratamento de Falhas na Fonte de Dados (ANS)
Durante a execução, o endpoint FTP/HTTP da ANS apresentou indisponibilidade intermitente e erros de conexão. Para garantir a testabilidade da aplicação e a integridade do pipeline, implementei um script (gerar_mock.py) que gera um cadastro base de operadoras a partir dos códigos encontrados nos arquivos de despesas. O script de transformação (src/transformacao.py) utiliza uma abordagem híbrida: tenta baixar os dados oficiais; se falhar, utiliza o cache local ou o mock gerado.

API Framework: FastAPI
Optei pelo FastAPI em detrimento do Flask devido à performance assíncrona e, principalmente, pela geração automática da documentação OpenAPI (Swagger). 

Frontend: Vue.js via CDN
Para a interface web, escolhi utilizar Vue.js importado via CDN (Single File Component).

Motivo: Evitar a complexidade de configuração de build tools (Webpack, Vite, NPM) para um projeto de escopo fechado. 

Banco de Dados: SQLite
Utilizei SQLite pela portabilidade. O banco é criado como um arquivo local, eliminando a necessidade do avaliador configurar serviços de banco de dados externos (como MySQL ou PostgreSQL) para rodar o teste. 

Estrutura do Projeto
Plaintext
Teste_Nicolas/
├── dados_brutos/          # Arquivos baixados e gerados (CSV/ZIP)
├── dados_processados/     # Arquivos finais do ETL
├── frontend/
│   └── index.html         # Aplicação Web (Vue.js)
├── sql/
│   ├── 1_create_tables.sql
│   └── 2_queries_analiticas.sql
├── src/
│   ├── api.py             # Aplicação FastAPI
│   ├── banco_de_dados.py  # Script de carga no SQLite
│   ├── gerar_mock.py      # Gerador de dados de teste
│   ├── main.py            # Crawler/Downloader
│   └── transformacao.py   # Lógica de limpeza e Join
├── README.md
└── requirements.txt