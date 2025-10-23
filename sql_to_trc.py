import sqlparse
def extract_clauses(sql_query):
    parsed = sqlparse.parse(sql_query)[0]
    tokens = [t for t in parsed.tokens if not t.is_whitespace]
    clauses = {"SELECT": None, "FROM": None, "WHERE": None}
    for i, token in enumerate(tokens):
        val = token.value.upper()
        if val.startswith("SELECT"):
            clauses["SELECT"] = tokens[i+1].value.strip()
        elif val.startswith("FROM"):
            clauses["FROM"] = tokens[i+1].value.strip()
        elif val.startswith("WHERE"):
            clauses["WHERE"] = " ".join(t.value for t in tokens[i+1:])
    return clauses

def sql_to_trc(sql_query):
    try:
        clauses = extract_clauses(sql_query)
        table = clauses["FROM"]
        projection = clauses["SELECT"]
        condition = clauses["WHERE"]

        # Handle COUNT and SELECT *
        if "COUNT" in projection.upper():
            projection = "count(*)"
        elif projection == "*":
            projection = "*"

        if condition:
            trc = f"{{t.{projection} | t ∈ {table} ∧ {condition}}}"
        else:
            trc = f"{{t.{projection} | t ∈ {table}}}"
        return trc

    except Exception as e:
        return None

# if __name__ == "__main__":
#     examples = [
#         "SELECT name FROM singer WHERE country = 'USA';",
#         "SELECT COUNT(*) FROM flight;",
#         "SELECT salary FROM employee;"
#     ]
#     for q in examples:
#         print(f"\nSQL: {q}\nTRC: {sql_to_trc(q)}")
