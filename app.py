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
    # Rule 3: IN (subquery) → EXISTS suggestion
    # -----------------------------------
    if re.search(r"\bIN\s*\(\s*SELECT", optimized_query, re.IGNORECASE):
        hints.append("Consider rewriting IN (subquery) as EXISTS for better performance on large datasets.")

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
    if re.search(r"(LOWER|UPPER|TRIM|SUBSTR|ROUND)\(", optimized_query, re.IGNORECASE):
        hints.append("Function applied to column in WHERE clause – may prevent index usage.")

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

import re

def parse_explain_text(raw_rows):
    structured = []

    for row in raw_rows:
        text = row[0] if isinstance(row, tuple) else str(row)

        # Remove arrow "->"
        text = text.lstrip("-> ").strip()

        # Split metrics (everything in parentheses)
        parts = re.split(r"\((?=[a-z])", text)  # split at "(cost=", "(actual time=" etc.
        step_part = parts[0].strip()
        metrics = " ".join("(" + p for p in parts[1:]) if len(parts) > 1 else ""

        # Extract cost, rows, actual time, loops
        cost = rows = act_time = act_rows = loops = ""
        if "cost=" in metrics:
            m = re.search(r"cost=([\d\.]+)", metrics)
            if m: cost = m.group(1)
        if "rows=" in metrics:
            m = re.findall(r"rows=(\d+)", metrics)
            if m:
                rows = m[0]          # estimated rows (first occurrence)
                if len(m) > 1:
                    act_rows = m[-1] # actual rows (last occurrence)
        if "actual time=" in metrics:
            m = re.search(r"actual time=([\d\.\.]+)", metrics)
            if m: act_time = m.group(1)
        if "loops=" in metrics:
            m = re.search(r"loops=(\d+)", metrics)
            if m: loops = m.group(1)

        # Detect condition (inside step_part after colon)
        condition = ""
        if ":" in step_part:
            step_name, condition = step_part.split(":", 1)
            step_part = step_name.strip()
            condition = condition.strip()

        structured.append({
            "Step": step_part,
            "Condition": condition,
            "Cost": cost,
            "Est. Rows": rows,
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
