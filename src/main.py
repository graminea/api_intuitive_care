from flask import Flask, request, jsonify
import psycopg2
import os
from flask_cors import CORS

app = Flask(__name__)

CORS(app)

def get_db_connection():
# Connect to PostgreSQL
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    return conn

@app.route("/search", methods=["GET"])
def search():
    conn = get_db_connection()
    cursor = conn.cursor()
    table = "relatorio_cadop"
    column = request.args.get("column", "reg_ans")   # Default column
    value = request.args.get("value", "")  # Search term

    # Secure table and column filtering
    allowed_tables = ["relatorio_cadop"]
    allowed_columns = [
        "reg_ans", 
        "razao_social", 
        "cnpj", 
        "nome_fantasia"
    ]

    if table not in allowed_tables or column not in allowed_columns:
        return jsonify({"error": "Invalid table or column"}), 400

    if column == "reg_ans" or column == "cnpj":
        query = f'SELECT * FROM {table} WHERE {column} = %s'
        cursor.execute(query, (value, ))

        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        results = [dict(zip(col_names, row)) for row in rows]

        return jsonify({
            "results": results
        })
    
    else:
    # Use ILIKE for partial case-insensitive match
        query = f'SELECT * FROM {table} WHERE {column} ILIKE %s'
        cursor.execute(query, (f"%{value}%",))

        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        results = [dict(zip(col_names, row)) for row in rows]

        return jsonify({
            "results": results
        })


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

