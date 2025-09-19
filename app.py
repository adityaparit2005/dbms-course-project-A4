from flask import Flask, render_template, request
import pymysql
import calendar
import re

app = Flask(__name__)

# Database connection config
db_config = {
    'host': 'localhost',
    'user': 'root',          # change this
    'password': 'password',  # change this
    'database': 'testdb',
    'cursorclass': pymysql.cursors.Cursor
}

# -----------------------------
# 1. RULE ENGINE
# -----------------------------
import re

def optimize_sql(query, conn):
    """
    Apply rule-based optimizations to SQL query.
    Rewrites when safe, otherwise appends suggestions.
    """

    optimized_query = query.strip()
    hints = []  # collect non-intrusive suggestions here

    # -----------------------------------
    # Rule 1: SELECT * → expand explicit columns
    # -----------------------------------
    if re.search(r"select\s+\*", optimized_query, re.IGNORECASE):
        try:
            match = re.search(r"from\s+([a-zA-Z0-9_]+)", optimized_query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                with conn.cursor() as cursor:
                    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
                    columns = [row[0] for row in cursor.fetchall()]
                column_list = ", ".join(columns)
                optimized_query = re.sub(
                    r"select\s+\*",
                    f"SELECT {column_list}",
                    optimized_query,
                    flags=re.IGNORECASE
                )
        except Exception:
            hints.append("SELECT * detected – explicitly list only required columns for performance.")

    # -----------------------------------
    # Rule 2: YEAR(col) = value → BETWEEN
    # -----------------------------------
    year_match = re.search(r"YEAR\((\w+)\)\s*=\s*(\d{4})", optimized_query, re.IGNORECASE)
    if year_match:
        col, year = year_match.groups()
        optimized_query = re.sub(
            r"YEAR\(\w+\)\s*=\s*\d{4}",
            f"{col} BETWEEN '{year}-01-01' AND '{year}-12-31'",
            optimized_query,
            flags=re.IGNORECASE,
        )

     # -----------------------------------
    # Rule 2: MONTH(col) = value → BETWEEN
    # -----------------------------------
  
    month_match = re.search(r"month\s*\(\s*(\w+)\s*\)\s*=\s*(\d{1,2})", optimized_query, re.IGNORECASE)
    if month_match:
        col, month_str = month_match.groups()
        month = int(month_str)
        year = 2025  # can be dynamic
        last_day = calendar.monthrange(year, month)[1]
        optimized_query = re.sub(
            r"month\s*\(\s*\w+\s*\)\s*=\s*\d{1,2}",
            f"{col} BETWEEN '{year}-{month:02d}-01' AND '{year}-{month:02d}-{last_day:02d}'",
            optimized_query,
            flags=re.IGNORECASE
        )

    # -----------------------------------
    # Rule 3: IN (subquery) → EXISTS rewrite (unified for alias/no-alias)
    # -----------------------------------
    in_match = re.search(
        r"(?:([\w]+)\.)?(\w+)\s+IN\s*\(\s*SELECT\s+([\w\.]+)\s+FROM\s+(\w+)(?:\s+(\w+))?(.*?)(?:\))",
        optimized_query,
        re.IGNORECASE | re.DOTALL,
    )

    if in_match:
        outer_alias, outer_col, inner_col, inner_table, alias, rest = in_match.groups()
        rest = rest.strip()

        # Use alias if provided, else fallback to table name
        alias = alias or inner_table

        # Build join condition safely
        if re.search(r"\bWHERE\b", rest, re.IGNORECASE):
            join_condition = f" AND {inner_col} = {outer_alias+'.' if outer_alias else ''}{outer_col}"
        else:
            join_condition = f" WHERE {alias}.{inner_col} = {outer_alias+'.' if outer_alias else ''}{outer_col}"

        # Clean up multiple WHEREs
        rest = re.sub(r"^\s*WHERE", " WHERE", rest, flags=re.IGNORECASE)

        exists_clause = f"EXISTS (SELECT 1 FROM {inner_table} {alias}{rest}{join_condition})"

        optimized_query = re.sub(
            r"(?:[\w]+\.)?\w+\s+IN\s*\(\s*SELECT.*?\)",
            exists_clause,
            optimized_query,
            flags=re.IGNORECASE | re.DOTALL,
        )

        hints.append("Rewrote IN (subquery) as EXISTS for improved performance.")



    # -----------------------------------
    # Rule 4: OR conditions → UNION suggestion
    # -----------------------------------
    if re.search(r"\sOR\s", optimized_query, re.IGNORECASE):
        hints.append("Multiple OR conditions detected – consider UNION ALL or indexes for better optimization.")

    # -----------------------------------
    # Rule 5: DISTINCT + GROUP BY redundancy
    # -----------------------------------
    if re.search(r"distinct", optimized_query, re.IGNORECASE) and re.search(r"group\s+by", optimized_query, re.IGNORECASE):
        hints.append("DISTINCT + GROUP BY both detected – you may not need both.")

    # -----------------------------------
    # Rule 6: ORDER BY without LIMIT
    # -----------------------------------
    order_match = re.search(r"\border\s+by\b", optimized_query, re.IGNORECASE)
    if order_match and "limit" not in optimized_query.lower():
        hints.append("ORDER BY without LIMIT – may cause unnecessary full sort on large datasets.")
        
        # Extract table name from FROM clause (simple regex, assumes single table)
        table_match = re.search(r"\bfrom\s+([`]?[\w]+[`]?)", optimized_query, re.IGNORECASE)
        if table_match:
            table_name = table_match.group(1).strip('`')
            try:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    total_rows = cursor.fetchone()[0]
                    
            except Exception:
                dynamic_limit = 1000  # fallback
        else:
            dynamic_limit = 1000  # fallback if table not found

        optimized_query = optimized_query.strip().rstrip(";") + f" LIMIT {total_rows};"
        hints.append(f"Applied dynamic LIMIT of {total_rows} to optimize sorting.")

        

    # -----------------------------------
    # Rule 7: LIKE with leading wildcard
    # -----------------------------------
    if re.search(r"like\s+'%[^']*'", optimized_query, re.IGNORECASE):
        hints.append("LIKE with leading wildcard ('%value') detected – index cannot be used efficiently.")

    # -----------------------------------
    # Rule 8: Functions on indexed column in WHERE
    # -----------------------------------
  
    func_match = re.search(
        r"(LOWER|UPPER|TRIM|SUBSTR|ROUND|INSTR)\s*\(\s*([\w\.]+)(?:\s*,\s*'([^']+)')?(?:\s*,\s*(\d+))?\)",
        optimized_query,
        re.IGNORECASE,
    )

    if func_match:
        func, col, str_val, arg_num = func_match.groups()
        func = func.upper()
        col = col.strip()

        if func == "LOWER":
            optimized_query = re.sub(
                rf"{func}\(\s*{col}\s*\)\s*=\s*'([^']+)'",
                lambda m: f"{col} = LOWER('{m.group(1)}')",
                optimized_query,
                flags=re.IGNORECASE,
            )
            hints.append(f"Rewrote LOWER({col}) for index usage.")

        elif func == "UPPER":
            optimized_query = re.sub(
                rf"{func}\(\s*{col}\s*\)\s*=\s*'([^']+)'",
                lambda m: f"{col} = UPPER('{m.group(1)}')",
                optimized_query,
                flags=re.IGNORECASE,
            )
            hints.append(f"Rewrote UPPER({col}) for index usage.")

        elif func == "TRIM":
            optimized_query = re.sub(
                rf"{func}\(\s*{col}\s*\)\s*=\s*'([^']+)'",
                lambda m: f"{col} = TRIM('{m.group(1)}')",
                optimized_query,
                flags=re.IGNORECASE,
            )
            hints.append(f"Rewrote TRIM({col}) for index usage.")

        elif func == "SUBSTR":
            optimized_query = re.sub(
                rf"SUBSTR\(\s*{col}\s*,\s*(\d+)\s*,\s*(\d+)\)\s*=\s*'([^']+)'",
                lambda m: (
                    f"{col} LIKE '{m.group(3)}%'" if m.group(1) == "1"
                    else f"{col} LIKE '{'_'*(int(m.group(1))-1)}{m.group(3)}%'"
                ),
                optimized_query,
                flags=re.IGNORECASE,
            )
            hints.append(f"Rewrote SUBSTR({col},start,len) for index usage.")

        elif func == "ROUND":
            optimized_query = re.sub(
                rf"ROUND\(\s*{col}\s*\)\s*=\s*(\d+)",
                lambda m: f"{col} BETWEEN {int(m.group(1))-0.5} AND {int(m.group(1))+0.5}",
                optimized_query,
                flags=re.IGNORECASE,
            )
            hints.append(f"Rewrote ROUND({col}) for index usage.")

        elif func == "INSTR":
            # INSTR(col,'str') > 0 → col LIKE '%str%'
            optimized_query = re.sub(
                rf"INSTR\(\s*{col}\s*,\s*'([^']+)'\s*\)\s*>\s*0",
                lambda m: f"{col} LIKE '%{m.group(1)}%'",
                optimized_query,
                flags=re.IGNORECASE,
            )
            hints.append(f"Rewrote INSTR({col},'{str_val}') > 0 → {col} LIKE '%{str_val}%' for index usage.")


    # -----------------------------------
    # Rule 9: COUNT(*) misuse
    # -----------------------------------
    if re.search(r"count\s*\(\s*\*\s*\)", optimized_query, re.IGNORECASE) and "group by" not in optimized_query.lower():
        hints.append("COUNT(*) used – if only checking existence, use EXISTS instead for efficiency.")

    # -----------------------------------
    # Rule 10: Cartesian product risk
    # -----------------------------------
    if "join" in optimized_query.lower() and "on" not in optimized_query.lower():
        hints.append("JOIN without ON clause – possible Cartesian product, check if intended.")

    # -----------------------------------
    # Attach hints as comments if any
    # -----------------------------------
    if hints:
        optimized_query += "\n-- " + "\n-- ".join(hints)

    return optimized_query

def parse_explain_text(raw_rows):
    structured = []

    for row in raw_rows:
        # Convert row to string if tuple
        text = row[0] if isinstance(row, tuple) else str(row)

        # Split multi-line plans into separate lines
        lines = text.split("\n")

        for line in lines:
            line = line.rstrip()
            if not line.strip():
                continue

            # Count indentation
            indent_level = 0
            stripped = line
            while stripped.startswith(" "):
                indent_level += 1
                stripped = stripped[1:]
            if stripped.startswith("->"):
                indent_level += 1
                stripped = stripped[2:].strip()

            # Extract only the operation name for Step
            step_name_match = re.match(r"([a-zA-Z ]+(?:on <[^>]+>)?)", stripped)
            if step_name_match:
                step_name = step_name_match.group(1).strip()
            else:
                step_name = stripped.split(":")[0].strip()

            # Everything else is Condition
            condition = stripped[len(step_name):].strip().lstrip(":").strip()

            # Extract metrics
            cost = est_rows = act_time = act_rows = loops = ""
            m = re.search(r"cost=([\d\.]+)", line)
            if m: cost = m.group(1)
            rows = re.findall(r"rows=(\d+)", line)
            if rows:
                est_rows = rows[0]
                if len(rows) > 1: act_rows = rows[-1]
            m = re.search(r"actual time=([\d\.\.]+)", line)
            if m: act_time = m.group(1)
            m = re.search(r"loops=(\d+)", line)
            if m: loops = m.group(1)

            structured.append({
                "Indent": indent_level,
                "Step": step_name,
                "Condition": condition,
                "Cost": cost,
                "Est. Rows": est_rows,
                "Actual Time": act_time,
                "Actual Rows": act_rows,
                "Loops": loops
            })

    return structured






# -----------------------------
# 2. PARSE EXPLAIN PLAN
# -----------------------------
# def parse_explain(cursor):
#     rows = cursor.fetchall()
#     columns = [desc[0] for desc in cursor.description]
#     return rows, columns


# -----------------------------
# 3. EXECUTE EXPLAIN
# -----------------------------
def analyze_query(query):
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    try:
        try:
            cursor.execute("EXPLAIN ANALYZE " + query)
        except:
            cursor.execute("EXPLAIN " + query)

        raw_rows = cursor.fetchall()
        structured = parse_explain_text(raw_rows)

        return {
            "query": query,
            "plan": structured
        }

    except Exception as e:
        return {"query": query, "error": str(e)}

    finally:
        cursor.close()
        connection.close()




# -----------------------------
# 4. ROUTES
# -----------------------------
@app.route('/')
def home():
    return render_template('index.html')


@app.route('/run_query', methods=['POST'])
def run_query():
    user_query = request.form['query'].strip()

    # Original query
    original = analyze_query(user_query)

    # Optimized query
    conn = pymysql.connect(**db_config)
    optimized_sql = optimize_sql(user_query, conn)
    conn.close()
    optimized = analyze_query(optimized_sql)

    return render_template(
        'index.html',
        original=original,
        optimized=optimized
    )


if __name__ == '__main__':
    app.run(debug=True)
