from flask import Flask, render_template, request
import pymysql
import calendar
import re
import datetime


app = Flask(__name__)


# --- STATIC DB CONFIGURATIONS (MUST BE UPDATED) ---
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'aditya', 
    'database': 'testdb',
    'port': 3306,
    'cursorclass': pymysql.cursors.Cursor
}
db1_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'aditya', 
    'database': 'testdb',
    'port': 3306,
    'cursorclass': pymysql.cursors.Cursor
}
# --- END STATIC CONFIGURATIONS ---


# Default weights for the composite efficiency score
TIME_WEIGHT = 0.6
COST_WEIGHT = 0.4


# -----------------------------
# 1. RULE ENGINE
# -----------------------------


def optimize_sql(query, conn):
    """
    Apply rule-based optimizations to SQL query.
    Rewrites when safe, otherwise appends suggestions.
    """


    optimized_query = query.strip()
    hints = [] 
    total_rows = 0 


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
# Rule 2: YEAR() / MONTH() → BETWEEN 
# -----------------------------------

    year_month_match = re.findall(
        r"(YEAR|MONTH)\s*\(\s*(\w+)\s*\)\s*=\s*(\d{1,4})",
        optimized_query,
        re.IGNORECASE,
    )

    if year_month_match:
        column_years = {}
        column_months = {}

        for func, col, num_str in year_month_match:
            func, col, num = func.upper(), col.strip(), int(num_str)
            if func == "YEAR" and 1900 <= num <= 2100:
                column_years[col] = num
            elif func == "MONTH" and 1 <= num <= 12:
                column_months[col] = num

        for col in set(list(column_years.keys()) + list(column_months.keys())):
            if col in column_years and col in column_months:
                year = column_years[col]
                month = column_months[col]
                last_day = calendar.monthrange(year, month)[1]
                combined_clause = f"{col} BETWEEN '{year}-{month:02d}-01' AND '{year}-{month:02d}-{last_day:02d}'"

                optimized_query = re.sub(
                    rf"YEAR\s*\(\s*{col}\s*\)\s*=\s*\d{{4}}\s*(AND|OR)?\s*MONTH\s*\(\s*{col}\s*\)\s*=\s*\d{{1,2}}",
                    combined_clause,
                    optimized_query,
                    flags=re.IGNORECASE,
                )
                hints.append(f"Combined YEAR({col}) and MONTH({col}) into single BETWEEN range for better performance.")

            elif col in column_years and col not in column_months:
                year = column_years[col]
                optimized_query = re.sub(
                    rf"YEAR\s*\(\s*{col}\s*\)\s*=\s*{year}",
                    f"{col} BETWEEN '{year}-01-01' AND '{year}-12-31'",
                    optimized_query,
                    flags=re.IGNORECASE,
                )
                hints.append(f"Rewrote YEAR({col}) = {year} into BETWEEN for index usage.")

            elif col in column_months and col not in column_years:
                month = column_months[col]
                year = datetime.date.today().year
                last_day = calendar.monthrange(year, month)[1]
                optimized_query = query
                hints.append(f"Rewrite MONTH({col}) = {month} to BETWEEN  for index usage .")


    # -----------------------------------
    # Rule 3: IN (subquery) → EXISTS rewrite
    # -----------------------------------

    
    in_clause_regex = r"""
        (?:([\w\.]+)\.)?              
        (\w+)\s+IN\s*\(              
        \s*SELECT\s+([\w\.]+)\s+    
        FROM\s+(\w+)                 
        (?:\s+(?!WHERE\b)(\w+))?      
        (?:\s+WHERE\s+(.*?))?         
        \s*\)                        
    """

    in_match = re.search(in_clause_regex, optimized_query, re.IGNORECASE | re.VERBOSE | re.DOTALL)

    if in_match:
        outer_alias, outer_col, inner_col, inner_table, alias, where_contents = in_match.groups()

        subquery_alias = alias or inner_table

        outer_column_ref = f"{outer_alias}.{outer_col}" if outer_alias else outer_col

        if not outer_alias:
            try:
                prior_text = optimized_query[:in_match.start()]
                from_iter = list(re.finditer(r"\bFROM\s+([`]?[-\w\.]+[`]?)(?:\s+AS\s+([`]?[-\w]+[`]?))?", prior_text, re.IGNORECASE))
                if from_iter:
                    last_from = from_iter[-1]
                    table_token = last_from.group(1)
                    existing_alias = last_from.group(2)
                    if not existing_alias:
                        generated_outer_alias = f"o_{re.sub(r'[^0-9a-zA-Z]', '_', outer_col)}"
                        start, end = last_from.span()
                        replacement = f"FROM {table_token} AS {generated_outer_alias}"
                        prior_text = prior_text[:start] + replacement + prior_text[end:]
                        optimized_query = prior_text + optimized_query[in_match.start():]
                        outer_alias = generated_outer_alias
                        outer_column_ref = f"{outer_alias}.{outer_col}"
            except Exception:
                pass

        inner_col_name = inner_col.split('.')[-1]

        join_condition = f"{subquery_alias}.{inner_col_name} = {outer_column_ref}"

        if where_contents and where_contents.strip():
            final_subquery_content = f"WHERE {where_contents.strip()} AND {join_condition}"
        else:
            final_subquery_content = f"WHERE {join_condition}"

        alias_str = f" {alias}" if alias else ""
        exists_clause = f"EXISTS (SELECT 1 FROM {inner_table}{alias_str} {final_subquery_content})"

        full_match_text = in_match.group(0)
        optimized_query = optimized_query.replace(full_match_text, exists_clause, 1)

        hints.append("Rewrote IN (subquery) as EXISTS for improved performance.")




    # -----------------------------------
    # Rule 4: OR conditions → UNION suggestion
    # -----------------------------------
    or_block_match = re.search(
        r"(SELECT\s+.*?\s+FROM\s+\w+\s+WHERE\s+)(.+?)(;|$)",
        optimized_query,
        re.IGNORECASE | re.DOTALL,
    )

    if or_block_match:
        select_clause, where_conditions, end_symbol = or_block_match.groups()

        if re.search(r"\s+or\s+", where_conditions, re.IGNORECASE) and not re.search(r"\bAND\b|\(|\)", where_conditions, re.IGNORECASE):
            conditions = re.split(r"\s+or\s+", where_conditions, flags=re.IGNORECASE)

            union_parts = [f"{select_clause}{cond.strip()}" for cond in conditions]
            optimized_query = " UNION ALL ".join(union_parts)

            optimized_query = optimized_query.strip()
            if not optimized_query.endswith(";"):
                optimized_query += ";"

            hints.append("Rewrote OR conditions as UNION ALL for better index utilization.")



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
        
        table_match = re.search(r"\bfrom\s+([`]?[\w]+[`]?)", optimized_query, re.IGNORECASE)
        if table_match:
            table_name = table_match.group(1).strip('`')
            try:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    total_rows = cursor.fetchone()[0]
                
            except Exception:
                total_rows = 1000 
        else:
            total_rows = 1000 


        optimized_query = optimized_query.strip().rstrip(";") + f" LIMIT {total_rows};"
        hints.append(f"Applied dynamic LIMIT of {total_rows} to optimize sorting.")


    # -----------------------------------
    # Rule 7: LIKE with leading wildcard
    # -----------------------------------
    if re.search(r"like\s+'%[^']*'", optimized_query, re.IGNORECASE):
        hints.append("LIKE with leading wildcard ('%value') detected – index cannot be used efficiently.")


    # -----------------------------------
    # Rule 8: Non-Sargable Function Rewrites 
    # -----------------------------------

    substr_pattern = r"SUBSTR\(\s*([\w\.]+)\s*,\s*(\d+)\s*,\s*(\d+)\)\s*=\s*'([^']+)'"
    def _substr_to_like(m):
        col_name = m.group(1)
        start_pos = int(m.group(2))
        literal = m.group(4)
        if start_pos <= 1:
            return f"{col_name} LIKE '{literal}%'"
        else:
            return f"{col_name} LIKE '{'_'*(start_pos-1)}{literal}%'"
    new_query, subs = re.subn(substr_pattern, _substr_to_like, optimized_query, flags=re.IGNORECASE)
    if subs:
        optimized_query = new_query
        hints.append("Rewrote SUBSTR(...) = '...' to LIKE when safe (uses '_' for fixed offset).")

    func_match = re.search(
        r"(LOWER|UPPER|SUBSTR|ROUND|INSTR)\s*\(\s*([\w\.]+)(?:\s*,\s*'([^']+)')?(?:\s*,\s*(\d+))?\)",
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

      
        elif func == "SUBSTR":
            optimized_query = re.sub(
                rf"{func}\(\s*{col}\s*,\s*(\d+)\s*,\s*(\d+)\)\s*=\s*'([^']+)'",
                lambda m:  f"{col} LIKE '{m.group(3)}%'" ,         
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


# -----------------------------
# 2. ANALYSIS AND METRICS
# -----------------------------
def analyze_query(query, db_conf):
    """
    Analyze SQL query execution using EXPLAIN ANALYZE.
    Returns (plan_data, total_time_ms, planner_cost).
    Handles UNION queries safely by wrapping them in a subquery.
    """

    custom_db_conf = db_conf.copy()
    custom_db_conf['cursorclass'] = pymysql.cursors.SSCursor

    connection = None
    cursor = None
    plan_data = []
    total_time = 0.0
    planner_cost = 0.0

    try:
        connection = pymysql.connect(**custom_db_conf)
        cursor = connection.cursor()

        # --- Clean and prepare query ---
        query = query.strip().rstrip(";")

      
        # Add EXPLAIN ANALYZE
        explain_query = "EXPLAIN ANALYZE " + query
        cursor.execute(explain_query)
        raw_rows = cursor.fetchall()

        # --- Parse EXPLAIN ANALYZE output ---
        for row_tuple in raw_rows:
            if not row_tuple or not row_tuple[0]:
                continue
            line = row_tuple[0]

            # Indentation (tree structure)
            indent_match = re.match(r"^(\s*)", line)
            indent = len(indent_match.group(1)) // 4 if indent_match else 0

            # Step, estimated cost and rows
            step_cost_match = re.search(
                r"->\s*(.*?)\s+\(cost=([\d\.]+)\s+rows=([\d\.]+)\)",
                line
            )

            # Actual times, actual rows, loops
            actuals_match = re.search(
                r"\(actual time=([\d\.]+)\.\.([\d\.]+)\s+rows=([\d\.]+)\s+loops=([\d\.]+)\)",
                line
            )

            step = step_cost_match.group(1).strip() if step_cost_match else line.strip()
            cost_str = step_cost_match.group(2) if step_cost_match else "0"
            est_rows = step_cost_match.group(3) if step_cost_match else "0"

            # Convert actual times safely to floats
            if actuals_match:
                try:
                    actual_time_start_f = float(actuals_match.group(1))
                    actual_time_end_f = float(actuals_match.group(2))
                    actual_rows_f = float(actuals_match.group(3))
                    loops_f = float(actuals_match.group(4))
                except ValueError:
                    actual_time_start_f = actual_time_end_f = actual_rows_f = loops_f = 0.0
            else:
                actual_time_start_f = actual_time_end_f = actual_rows_f = loops_f = 0.0

            # Combined string for display
            actual_time_str = f"{actual_time_start_f}..{actual_time_end_f}" if actuals_match else ""

            # Capture root total time and planner cost
            if indent == 0 and actual_time_end_f and total_time == 0.0:
                total_time = actual_time_end_f
            if indent == 0 and cost_str and planner_cost == 0.0:
                try:
                    planner_cost = float(cost_str)
                except ValueError:
                    planner_cost = 0.0

            # Append row to plan_data
            plan_data.append({
                "Indent": indent,
                "Step": step,
                "Cost": cost_str,
                "Est. Rows": est_rows,
                "Actual Time": actual_time_str,
                "Actual Rows": str(int(actual_rows_f)),
                "Loops": str(int(loops_f)),
                # Precompute padding for display (avoid template math)
                "PaddingLeftPx": 12 + (indent * 25),
            })

        # --- Fallback if EXPLAIN returns no lines ---
        if not plan_data:
            plan_data = [{"Step": "No plan returned. Query executed successfully but EXPLAIN output was empty (possibly UNION or complex query).", "PaddingLeftPx": 12}]

        return plan_data, total_time, planner_cost

    except Exception as e:
        # Return diagnostic row instead of silent failure
        return (
            [{"Step": f"EXPLAIN ANALYZE failed: {str(e)}", "PaddingLeftPx": 12}],
            0.0,
            0.0,
        )

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()




def calculate_efficiency_metrics(original, optimized):
    """Calculates time, cost, and composite efficiency."""
    metrics = {
        'time_efficiency': None,
        'cost_efficiency': None,
        'composite_score': None
    }
    
    time_A = original.get('total_time_ms', 0)
    time_B = optimized.get('total_time_ms', 0)
    
    cost_A = original.get('planner_cost', 0)
    cost_B = optimized.get('planner_cost', 0)

    if cost_A == 0 and cost_B > 0:
        metrics['cost_efficiency'] = 0.000001
        cost_A = 0.000001

    if time_A > 0 and time_B > 0:
        metrics['time_efficiency'] = ((time_A - time_B) / time_A) * 100
    
    if cost_A > 0 and cost_B > 0:
        metrics['cost_efficiency'] = ((cost_A - cost_B) / cost_A) * 100
        
    if metrics['time_efficiency'] is not None and metrics['cost_efficiency'] is not None:
        metrics['composite_score'] = (
            (metrics['time_efficiency'] * TIME_WEIGHT) + 
            (metrics['cost_efficiency'] * COST_WEIGHT)
        )
    

         
    return metrics


# -----------------------------
# 3. ROUTES
# -----------------------------
@app.route('/')
def home():
    return render_template('index.html', db_config=db_config, db1_config=db1_config)

@app.route('/schema')
def schema():
    return render_template('schema.html')


@app.route('/run_query', methods=['POST'])
def run_query():
    user_query = request.form['query'].strip()

    original_plan, original_time, original_cost = analyze_query(user_query, db_config)
    original_data = {
        'query': user_query, 'plan': original_plan, 
        'total_time_ms': original_time, 'planner_cost': original_cost
    }

    conn = None
    optimized_sql = user_query 
    try:
        conn = pymysql.connect(**db1_config)
        optimized_sql = optimize_sql(user_query, conn)
    except Exception as e:
        optimized_sql += f"\n-- Optimization setup failed: {str(e)}"
    finally:
        if conn: conn.close()
    
    optimized_query_base = optimized_sql.split('--')[0].strip()
    optimized_plan, optimized_time, optimized_cost = analyze_query(optimized_query_base, db1_config)
    
    optimized_data = {
        'query': optimized_sql, 'plan': optimized_plan, 
        'total_time_ms': optimized_time, 'planner_cost': optimized_cost
    }

    # Precompute values that are more reliable/safe to access from the template
    # Split out base optimized query and any appended hints (lines starting with --)
    hints_raw = optimized_sql.split('--')[1:]
    hints_list = [h.strip() for h in hints_raw if h and h.strip()]
    optimized_data['query_base'] = optimized_query_base
    optimized_data['hints_list'] = hints_list

    efficiency_metrics = calculate_efficiency_metrics(original_data, optimized_data)

    # Expose metric pieces separately for simpler template usage
    composite_score = efficiency_metrics.get('composite_score')
    time_efficiency = efficiency_metrics.get('time_efficiency')
    cost_efficiency = efficiency_metrics.get('cost_efficiency')

    # Choose a display CSS class server-side to avoid inline styles in template
    if composite_score is None:
        display_class = 'neutral'
    else:
        display_class = 'good' if composite_score > 0 else ('bad' if composite_score < 0 else 'neutral')

    return render_template(
        'index.html',
        original=original_data,
        optimized=optimized_data,
        efficiency_metrics=efficiency_metrics,
        composite_score=composite_score,
        time_efficiency=time_efficiency,
        cost_efficiency=cost_efficiency,
    display_class=display_class,
        db_config=db_config,
        db1_config=db1_config
    )


if __name__ == '__main__':
    calendar.datetime = datetime 
    app.run(debug=True)
