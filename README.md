<h1 align="center">📈 <strong>airflow_rank_fiis</strong></h1>

<p align="center">
  Automação completa de ETL/ELT para extração, processamento e publicação de dados de Fundos Imobiliários.  
</p>

<hr/>

<h2>🎯 <strong>Objetivo</strong></h2>

<p>
O projeto <strong>airflow_rank_fiis</strong> foi criado para automatizar todo o processo de extração de dados de um ranking público de Fundos Imobiliários.  
Ele coleta a tabela diretamente do site de origem, trata os dados e publica tudo automaticamente em uma <strong>Google Sheet</strong>, garantindo informações sempre atualizadas para consulta ou dashboards.
</p>

<hr/>

<h2>⚙️ <strong>Principais Tecnologias</strong></h2>

<ul>
  <li><strong>Apache Airflow</strong> — Orquestração completa dos pipelines de ETL/ELT.</li>
  <li><strong>Docker Compose</strong> — Containerização e infraestrutura como código.</li>
  <li><strong>Selenium</strong> — Extração de dados a partir de página HTML carregada dinamicamente.</li>
  <li><strong>Pandas</strong> — Transformação e limpeza dos dados coletados.</li>
  <li><strong>Google Drive API</strong> — Envio automático dos resultados para uma planilha no Google Sheets.</li>
  <li><strong>PostgreSQL</strong> (opcional) — Armazenamento intermediário.</li>
</ul>

<hr/>

<h2>🧭 <strong>Fluxo do Projeto</strong></h2>

<ol>
  <li><strong>Airflow</strong> agenda e dispara o pipeline automaticamente.</li>
  <li><strong>Selenium</strong> acessa o site e captura o HTML renderizado com a tabela.</li>
  <li>O <strong>Pandas</strong> converte a tabela para DataFrame e executa transformações.</li>
  <li>Os dados são validados e preparados para exportação.</li>
  <li>A API do <strong>Google Drive</strong> atualiza a Google Sheet com o dataset processado.</li>
  <li>Tudo roda em ambiente containerizado via <strong>Docker Compose</strong>, garantindo reprodutibilidade.</li>
</ol>

<hr/>

<h2>💡 <strong>Por que usar este projeto</strong></h2>

<ul>
  <li>Automatiza tarefas repetitivas e elimina esforço manual.</li>
  <li>Pipeline confiável e agendado com Airflow.</li>
  <li>Dados sempre atualizados para relatórios e dashboards.</li>
  <li>Fácil de manter e reproduzir em qualquer ambiente.</li>
  <li>Arquitetura clara e modular para auditoria e extensões futuras.</li>
</ul>

<hr/>

<h2>📁 <strong>Estrutura do Repositório</strong></h2>

<pre>
/dags                 → Definições dos workflows do Airflow  
/data                 → Dados intermediários (quando aplicável)  
docker-compose.yaml   → Configuração da infraestrutura  
.env                  → Variáveis de ambiente  
README.md             → Este documento  
</pre>

<hr/>

<h3 align="center">🚀 Projeto ideal para quem quer processos de dados automatizados, organizados e atualizados.</h3>
