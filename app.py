from flask import Flask, render_template, request
from prettytable import PrettyTable
import pymysql
import re

app = Flask(__name__)

# Database connection config
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password',
    'database': 'testdb',
    'cursorclass': pymysql.cursors.Cursor
}


def parse_explain_analyze(output):
    """
    Parse EXPLAIN ANALYZE text output into a structured table.
    Handles indentation (nested operations) and extracts
    cost, rows, time, etc.
    """
 
    # If output is a tuple, flatten it into a string
    if isinstance(output, tuple):
        output = " ".join(str(x) for x in output if x)

    # Regex to match operation lines
    line_pattern = re.compile(
        r'(?P<operation>.+?)\s*'
        r'\(cost=(?P<cost>[\d\.]+).*rows=(?P<est_rows>[\d\.]+)\)\s*'
        r'\(actual time=(?P<actual_time>[\d\.]+..[\d\.]+).*rows=(?P<actual_rows>[\d\.]+).*loops=(?P<loops>\d+)\)'
    )

    table = PrettyTable()
    table.field_names = ["Operation", "Estimated Cost", "Estimated Rows", "Actual Time", "Actual Rows", "Loops"]

    for line in output.splitlines():
        match = line_pattern.search(line.strip())
        if match:
            indent_level = len(line) - len(line.lstrip())
            operation = " " * indent_level + match.group("operation")

            table.add_row([
                operation,
                match.group("cost"),
                match.group("est_rows"),
                match.group("actual_time"),
                match.group("actual_rows"),
                match.group("loops")
            ])

    return table



@app.route('/')
def home():
    return render_template('index.html')

@app.route('/run_query', methods=['POST'])
def run_query():
    query = request.form['query'].strip()
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    try:
        cursor.execute(query)

        # ✅ Case 1: EXPLAIN ANALYZE (special parsing)
        if query.lower().startswith("explain analyze"):
            raw_result = cursor.fetchall()
            parsed_rows = parse_explain_analyze(raw_result)

            if parsed_rows:
                columns = ["Operation", "Estimated Cost", "Estimated Rows", "Actual Time", "Actual Rows", "Loops"]
                return render_template('index.html', result=parsed_rows, columns=columns)
            else:
                # fallback: show raw text if parsing fails
                plan_text = "\n".join([row[0] for row in raw_result])
                return render_template('index.html', explain_output=plan_text)

        # ✅ Case 2: SELECT / SHOW / EXPLAIN
        elif query.lower().startswith(("select", "show", "explain")):
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return render_template('index.html', result=result, columns=columns)

        # ✅ Case 3: INSERT / UPDATE / DELETE
        else:
            connection.commit()
            return render_template('index.html', message="✅ Query Executed Successfully")

    except Exception as e:
        return render_template('index.html', message=f"❌ Error: {str(e)}")

    finally:
        cursor.close()
        connection.close()

if __name__ == '__main__':
    app.run(debug=True)
