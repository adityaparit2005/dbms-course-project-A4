from flask import Flask, render_template, request
import pymysql
import re

app = Flask(__name__)

# Database connection config
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'aditya ',
    'database': 'testdb',
    'cursorclass': pymysql.cursors.Cursor
}

def parse_explain_analyze_html(raw_result):
    """
    Parse EXPLAIN ANALYZE output (a list of single-line tuples) into an HTML table.
    """
    line_pattern = re.compile(
        r'(?P<operation>.+?)\s*'
        r'\(cost=(?P<cost>[\d\.]+).*rows=(?P<est_rows>[\d\.]+)\)\s*'
        r'\(actual time=(?P<actual_time>[\d\.]+..[\d\.]+).*rows=(?P<actual_rows>[\d\.]+).*loops=(?P<loops>\d+)\)'
    )
    # Flatten result to a single string per line
    output_text = "\n".join(str(row[0]) for row in raw_result if row and row[0])

    rows = []
    for line in output_text.splitlines():
        match = line_pattern.search(line.strip())
        if match:
            indent_level = len(line) - len(line.lstrip())
            operation = "&nbsp;" * indent_level * 2 + match.group("operation")
            rows.append([
                operation,
                match.group("cost"),
                match.group("est_rows"),
                match.group("actual_time"),
                match.group("actual_rows"),
                match.group("loops")
            ])

    if not rows:
        return None

    # Build HTML table
    html = """
    <table border="1" cellspacing="0" cellpadding="5">
        <tr>
            <th>Operation</th>
            <th>Estimated Cost</th>
            <th>Estimated Rows</th>
            <th>Actual Time</th>
            <th>Actual Rows</th>
            <th>Loops</th>
        </tr>
    """
    for row in rows:
        html += "<tr>" + "".join(f"<td>{col}</td>" for col in row) + "</tr>"
    html += "</table>"
    return html

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

        # EXPLAIN ANALYZE special parsing
        if query.lower().startswith("explain analyze"):
            raw_result = cursor.fetchall()
            parsed_html = parse_explain_analyze_html(raw_result)
            if parsed_html:
                return render_template('index.html', raw_html=parsed_html)
            else:
                # fallback: show as plain text
                plan_text = "\n".join([row[0] for row in raw_result])
                return render_template('index.html', explain_output=plan_text)

        # SELECT, SHOW, or EXPLAIN (simple table display)
        elif query.lower().startswith(("select", "show", "explain")):
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return render_template('index.html', result=result, columns=columns)

        # INSERT/UPDATE/DELETE confirmation
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
