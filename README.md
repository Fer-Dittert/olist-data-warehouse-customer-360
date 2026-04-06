# Customer 360 Analytics | Olist Dataset

Projeto de Engenharia de Dados e Analytics Engineering com foco na construção de uma **visão 360° do cliente**, utilizando **dbt, Amazon Redshift e S3**, com modelagem dimensional baseada em **Kimball**.

---

## 🎯 Objetivo

Construir um Data Warehouse analítico capaz de consolidar dados de vendas, pedidos, pagamentos, entregas e avaliações, permitindo uma visão completa do comportamento do cliente em um marketplace de e-commerce.

---

## 🏗️ Arquitetura

- **Amazon S3** → armazenamento dos dados brutos  
- **Amazon Redshift** → Data Warehouse  
- **dbt** → transformação e modelagem dos dados  
- **GitHub** → versionamento  

Fluxo:

S3 → Redshift (raw) → dbt (staging → intermediate → marts) → análises

---

## 📊 Modelagem de Dados

O modelo foi construído seguindo a abordagem **Kimball (Star Schema)**, com múltiplas granularidades:

### 🔹 Tabelas Fato

- **fct_vendas** → nível de item do pedido  
- **fct_pedidos** → nível de pedido  
- **fct_cliente_metricas** → nível de cliente  

### 🔹 Dimensões

- **dim_clientes**  
- **dim_produtos**  
- **dim_vendedores**  
- **dim_status_pedido**  
- **dim_data** (dimensão temporal)  
- **dim_localizacao** (dimensão geográfica)  

### 🔹 Camada Analítica

- **mart_cliente_360** → visão consolidada para consumo  

---

## 🧠 Visão 360 do Cliente

A modelagem permite analisar:

- comportamento de compra  
- valor total gasto  
- ticket médio  
- frequência de compra  
- satisfação (reviews)  
- tempo de entrega  
- atrasos logísticos  
- segmentação de clientes  
- risco de churn  

---

## 📈 Principais Métricas

- `valor_total_gasto`  
- `ticket_medio`  
- `media_dias_entrega`  
- `media_nota_avaliacao`  
- `total_pedidos_atrasados`  
- `indicador_risco_churn`  
- `segmento_frequencia`  
- `segmento_valor`  
- `segmento_satisfacao`  

---

## ⚙️ Estrutura do Projeto (dbt)

models/
│
├── staging/        → limpeza e padronização  
├── intermediate/   → regras de negócio e joins  
└── marts/          → fatos e dimensões finais  

---

## 🔄 Pipeline de Dados

1. Ingestão de dados no S3  
2. Carga no Redshift  
3. Transformações com dbt:  
   - limpeza (staging)  
   - enriquecimento (intermediate)  
   - modelagem dimensional (marts)  
4. Disponibilização para análise  

---

## 🚀 Diferenciais do Projeto

- Modelagem com múltiplas granularidades (item, pedido, cliente)  
- Separação clara de camadas (staging → intermediate → marts)  
- Dimensão de localização compartilhada (clientes e vendedores)  
- Construção de métricas de comportamento do cliente  
- Implementação de indicador de churn  
- Estrutura pronta para consumo em BI  

---

## 📌 Dataset

- Olist E-commerce Dataset  
- Dados públicos de marketplace brasileiro  

---

## 📎 Observações

Este projeto foi desenvolvido como parte de um trabalho acadêmico em Engenharia de Dados, com foco em boas práticas de modelagem e construção de pipelines analíticos.

---

 
