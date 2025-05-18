from flask import Flask, render_template, jsonify
from sys import path
path.append(r'C:\Program Files\Microsoft.NET\ADOMD.NET\150')
from pyadomd import Pyadomd
import pandas as pd
import numpy as np
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# SSAS connection parameters
model_name = 'CUBE_STORE_IMPORT'
database_name = 'MultidimensionalProject2'
server_name = r'WIN_LAP_TUNGNG\BTL_SSAS'

# Connection string
connection_string = f'Provider=MSOLAP;Data Source={server_name};Initial Catalog={database_name};'

# MDX query
mdx_query = """
SELECT 
    {[Measures].[Quantity Import]} ON COLUMNS,
    [Dim Item].[Item Description].[Item Description].MEMBERS ON ROWS
FROM 
    [CUBE_STORE_IMPORT]
"""

def get_ssas_data():
    try:
        with Pyadomd(connection_string) as con:
            with con.cursor().execute(mdx_query) as cur:
                rows = cur.fetchall()
                logger.debug(f"Number of rows fetched: {len(rows)}")
                columns = [desc.name for desc in cur.description]
                logger.debug(f"Columns: {columns}")
                df = pd.DataFrame(rows, columns=columns)
                logger.debug(f"DataFrame shape: {df.shape}")
                logger.debug(f"DataFrame head:\n{df.head()}")
                # Replace all NaN values with None before converting to dict
                df = df.replace({np.nan: None})
                result = df.to_dict('records')
                logger.debug(f"Number of records in result: {len(result)}")
                return result
    except Exception as e:
        logger.error(f"Error in get_ssas_data: {str(e)}")
        return {"error": str(e)}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    data = get_ssas_data()
    logger.debug(f"Data being returned: {data}")
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True) 