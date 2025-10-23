import json
import pandas as pd
from tqdm import tqdm
def load_json(path):
    with open(path, 'r') as f:
        return json.load(f)

print("datasets...")
spider_train = load_json('dataset/spider_data/train_spider.json')
spider_dev = load_json('dataset/spider_data/dev.json')

sparc_train = load_json('dataset/sparc/train.json')
sparc_dev = load_json('dataset/sparc/dev.json')

cosql_train = load_json('dataset/cosql_dataset/sql_state_tracking/cosql_train.json')
cosql_dev = load_json('dataset/cosql_dataset/sql_state_tracking/cosql_dev.json')

tables_json = load_json('dataset/spider_data/tables.json')

schema_map = {t['db_id']: t for t in tables_json}

def get_schema_context(db_id):
    schema = schema_map.get(db_id, None)
    if not schema:
        return {}
    tables = schema['table_names']
    columns = [
        f"{tables[t_idx]}.{col}" for t_idx, col in schema['column_names'] if t_idx != -1
    ]
    return {
        "tables": tables,
        "columns": columns,
        "foreign_keys": schema['foreign_keys'],
        "primary_keys": schema['primary_keys']
    }


def normalize_spider(data, source):
    return [
        {
            "source": source,
            "db_id": d['db_id'],
            "question": d['question'],
            "sql": d['query'],
            "schema": get_schema_context(d['db_id'])
        }
        for d in data
    ]

def normalize_sparc(data, source):
    rows = []
    for conv in data:
        db = conv['database_id']
        for turn in conv['interaction']:
            rows.append({
                "source": source,
                "db_id": db,
                "question": turn['utterance'],
                "sql": turn['query'],
                "schema": get_schema_context(db)
            })
    return rows

def normalize_cosql(data, source):
    rows = []
    for conv in data:
        db = conv['database_id']
        for turn in conv['interaction']:
            rows.append({
                "source": source,
                "db_id": db,
                "question": turn['utterance'],
                "sql": turn['query'],
                "schema": get_schema_context(db)
            })
    return rows

df_spider = normalize_spider(spider_train + spider_dev, "Spider")
df_sparc = normalize_sparc(sparc_train + sparc_dev, "SParC")
df_cosql = normalize_cosql(cosql_train + cosql_dev, "CoSQL")

combined = df_spider + df_sparc + df_cosql
df = pd.DataFrame(combined)

# print(f"Total combined samples: {len(df)}")

df['q_len'] = df['question'].apply(lambda x: len(x.split()))
df['sql_len'] = df['sql'].apply(lambda x: len(x.split()))

output_path = "combined_data.jsonl"
df.to_json(output_path, orient="records", lines=True)
print(f"Combined dataset saved to {output_path}")
