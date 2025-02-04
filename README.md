# pipe-duckdb-delta-aws
Pipeline de dados na aws utilizando tecnologia Single Node Duckdb e Lakehouse com delta

- para visualizar as extensões que estão disponíveis e seus status
- 
conn.sql(""" 

FROM duckdb_extensions()

""")

#delete data deltalake
categories_delta.delete('category_id > 4')

# Realizando merge (upsert) com deltalake
categories_delta.merge(
    source=df,
    predicate='target.category_id = source.category_id',
    source_alias='source',
    target_alias='target'
).when_matched_update_all().when_not_matched_insert_all().execute()

# Realizando update com deltalake
categories_delta.merge(
    source=df,
    predicate='target.category_id = source.category_id',
    source_alias='source',
    target_alias='target'
).when_matched_update(updates={'target.category_name': {'source.category_name'}}).execute()


# Vacuum: Realiza exclusão de arquivos deletadados
# Padrão de deleção: à partir do setimo dia 
# Você consegue forçar 

# categories_delta.vacuum(retention_hours=680, dry_run=False, enforce_retention_duration=True)

categories_delta.history()

categories_delta.get_add_actions(flatten=True).to_pandas()

categories_delta.optimize.compact() # redução de small files

categories_delta.optimize.z_order(columns='category_id')

categories_delta.load_as_version(0) # carregar versões antigas

categories_delta.restore(0, ignore_missing_files=True) # deixar na versão atual alguma versão anterior