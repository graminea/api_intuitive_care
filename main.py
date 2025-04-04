from flask import Flask, request, jsonify
import psycopg2
import os
from flask_cors import CORS

app = Flask(__name__)

CORS(app)

def get_db_connection():
# Connect to PostgreSQL
    DATABASE_URL = "postgresql://graminea:npg_4AJZu6SDTFEI@ep-square-sun-a4690bwt-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"
    conn = psycopg2.connect((DATABASE_URL))
    return conn

@app.route("/search", methods=["GET"])
def search():
    conn = get_db_connection()
    cursor = conn.cursor()
    table = "relatorio_cadop"
    column = request.args.get("column", "razao_social")   # Default column
    value = request.args.get("value", "")  # Search term
    limit = int(request.args.get("limit", 10))  # Results per page
    offset = int(request.args.get("offset", 0))  # Pagination offset

    # Secure table and column filtering
    allowed_tables = ["Relatorio_cadop"]
    allowed_columns = [
        "reg_ans", 
        "razao_social", 
        "cnpj", 
        "nome_fantasia"
    ]

    if table not in allowed_tables or column not in allowed_columns:
        return jsonify({"error": "Invalid table or column"}), 400

    try:
        # Use ILIKE for partial case-insensitive match
        query = f'SELECT * FROM {table} WHERE {column} ILIKE %s LIMIT %s OFFSET %s'
        cursor.execute(query, (f"%{value}%", limit, offset))

        rows = cursor.fetchall()
        col_names = [desc[0] for desc in cursor.description]
        results = [dict(zip(col_names, row)) for row in rows]

        return jsonify({
            "results": results,
            "next_offset": offset + limit,
            "prev_offset": max(0, offset - limit)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

