<h1>Projeto de Extração e Processamento de Dados</h1>

<h2>Objetivo</h2>
<p>
    Este projeto tem como objetivo extrair e processar dados de uma tabela em um site que contém um ranking de Fundos Imobiliários. Após a extração, os dados serão utilizados para criar uma Google Sheet em um repositório no Google Drive, após passar por etapas de ELT utilizando o Airflow.
</p>

<h2>Infraestrutura</h2>
<p>
    Para criar a infraestrutura como código, utilizei o Docker Compose com as seguintes imagens:
</p>
<ul>
    <li>Apache Airflow</li>
    <li>Selenium</li>
    <li>Redis</li>
    <li>Postgres</li>
</ul>

<h2>Metodologia</h2>
<p>
    Como o site não disponibiliza API e possui barreiras de consumo de dados via <code>curl</code> (Linux) para não impactar a performance do site, utilizei a seguinte abordagem:
</p>
<ol>
    <li>Baixar o arquivo HTML do site com o Selenium, já com os dados carregados.</li>
    <li>Fazer uma varredura no arquivo para encontrar a estrutura da tabela.</li>
    <li>Carregar os dados da tabela como um DataFrame no Pandas.</li>
    <li>Executar as etapas de ELT (Extração, Transformação e Carregamento).</li>
    <li>Carregar os dados em uma pasta do Google Drive como Google Sheets.</li>
</ol>
