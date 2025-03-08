# Data Lakehouse com Delta Lake, DuckDB e AWS

## Motivação
O avanço das arquiteturas de dados levou à popularização do conceito de Data Lakehouse, combinando a flexibilidade dos Data Lakes com a governança e desempenho dos Data Warehouses. Tecnologias como **Delta Lake** e **DuckDB** vêm se destacando nesse contexto, oferecendo maior confiabilidade, versionamento de dados e processamento otimizado para análises interativas.

Este projeto surgiu como uma extensão da minha experiência ao realizar o curso de [Data Lakehouse com Delta Lake, DuckDB e Azure Data Lake-Gen2](https://www.udemy.com/share/10bWT33@BOBgzoYYhPIzq4NVlNJFeTL06-c0YhU_NBLwfaIs7EbNpWzs0j-gWc-4RNptLrLf/). Originalmente, o curso utilizava **Microsoft Azure**, o que representou minha primeira experiência prática com esse provedor de cloud. Como aprimoramento, decidi reconstruir a solução na **AWS**, onde atuo no dia a dia, para oferecer uma alternativa para profissionais que desejam explorar o mesmo conceito em um ambiente diferente.

Além da migração para AWS, ampliei a solução incorporando outras tecnologias de analytics e aprimorando o código com **boas práticas de engenharia de software**, garantindo maior modularidade, legibilidade e eficiência.

## Arquitetura da Solução
_(Adicionar diagrama da arquitetura aqui)_

## Getting Started
Para executar este projeto, siga os passos abaixo:

1. Clone o repositório:
   ```bash
   git clone <URL_DO_REPOSITORIO>
   cd <NOME_DO_REPOSITORIO>
   ```
2. Crie um ambiente virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate  # No Windows: venv\Scripts\activate
   ```
3. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
4. Execute o pipeline de ingestão de dados:
   ```bash
   python ingestion_processor.py
   ```

## Principais Códigos com Delta Lake

### Merge (Upsert)
Realiza a fusão dos dados da tabela Delta com um DataFrame, atualizando registros existentes e inserindo novos quando não há correspondência.
```python
categories_delta.merge(
    source=df,
    predicate='target.category_id = source.category_id',
    source_alias='source',
    target_alias='target'
).when_matched_update_all().when_not_matched_insert_all().execute()
```

### Update
Atualiza valores específicos em registros existentes na tabela Delta com base nos dados do DataFrame.
```python
categories_delta.merge(
    source=df,
    predicate='target.category_id = source.category_id',
    source_alias='source',
    target_alias='target'
).when_matched_update(updates={'target.category_name': {'source.category_name'}}).execute()
```

### Vacuum
Remove arquivos antigos e deletados da tabela Delta para otimizar o armazenamento.
```python
categories_delta.vacuum(retention_hours=680, dry_run=False, enforce_retention_duration=True)
```

### Histórico de Alterações
Exibe o histórico de versões da tabela Delta.
```python
categories_delta.history()
```

### Ações de Adição
Retorna as ações de adição na tabela Delta, convertendo os dados para um DataFrame Pandas.
```python
categories_delta.get_add_actions(flatten=True).to_pandas()
```

### Otimização
Compacta pequenos arquivos para melhorar a eficiência da leitura.
```python
categories_delta.optimize.compact()
```

### Z-Ordering
Reorganiza os dados na tabela Delta para otimizar consultas por uma coluna específica.
```python
categories_delta.optimize.z_order(columns='category_id')
```

### Carregar Versão Antiga
Permite carregar uma versão anterior da tabela Delta.
```python
categories_delta.load_as_version(0)
```

### Restaurar Versão
Restaura a tabela para uma versão anterior, ignorando arquivos ausentes se necessário.
```python
categories_delta.restore(0, ignore_missing_files=True)
```

