
# SSAS connection parameters
# model_name = 'DW'
# database_name = 'MultidimensionalProject1'
# server_name = r'DESKTOP-RKQ2KCM\MSSQL2'

from flask import Flask, render_template, jsonify, request
import sys
import os
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Try multiple potential ADOMD.NET paths
adomd_paths = [
    r'C:\Program Files\Microsoft.NET\ADOMD.NET\160',
    r'C:\Program Files (x86)\Microsoft.NET\ADOMD.NET\160',
    r'C:\Program Files\Microsoft Analysis Services\ADOMD\160',
    os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Dependencies')
]

added_path = False
for path in adomd_paths:
    if os.path.exists(path):
        sys.path.append(path)
        logger.debug(f"Added ADOMD path: {path}")
        added_path = True

if not added_path:
    logger.error("Could not find ADOMD.NET directory in any expected location")

try:
    from pyadomd import Pyadomd
    logger.debug("Successfully imported Pyadomd")
except Exception as e:
    logger.error(f"Failed to import Pyadomd: {str(e)}")
    
import pandas as pd
import numpy as np
import json

app = Flask(__name__)

# SSAS connection parameters
model_name = 'DW'
database_name = 'MultidimensionalProject1'
server_name = r'DESKTOP-RKQ2KCM\MSSQL2'

# Connection string
connection_string = f'Provider=MSOLAP;Data Source={server_name};Initial Catalog={database_name};'

def execute_mdx(mdx_query):
    try:
        with Pyadomd(connection_string) as con:
            with con.cursor().execute(mdx_query) as cur:
                rows = cur.fetchall()
                logger.debug(f"Number of rows fetched: {len(rows)}")
                columns = [desc.name for desc in cur.description]
                logger.debug(f"Columns: {columns}")
                df = pd.DataFrame(rows, columns=columns)
                # Replace all NaN values with None before converting to dict
                df = df.replace({np.nan: None})
                result = df.to_dict('records')
                return result
    except Exception as e:
        logger.error(f"Error executing MDX: {str(e)}")
        return {"error": str(e)}

# Get cube metadata (dimensions and measures)
def get_cube_metadata():
    try:
        # Updated to match the actual cube structure from the screenshot
        dimensions = {
            "Dim Time": ["Year", "Quarter", "Month", "Day"],
            "Dim Store": ["Store Name", "Store ID", "City Name", "City ID", "State", "Office Address", "Phone Number"],
            "Dim Customer": ["Customer ID", "Customer Name"],
            "Dim Item": ["Item Description", "Item ID", "Item Size", "Item Weight", "Price"]
        }
        
        measures = [
            "Total Item Price",
            "Quantity Sale",
            "Quantity Ordered"
        ]
        
        return jsonify({"dimensions": dimensions, "measures": measures})
    except Exception as e:
        logger.error(f"Error getting cube metadata: {str(e)}")
        return jsonify({"error": str(e)})

# Build dynamic MDX based on parameters
def build_mdx_query(dimensions_on_rows, dimensions_on_cols, measures, filters=None):
    if not dimensions_on_rows and not dimensions_on_cols:
        return None
    
    # Build COLUMNS part
    if measures and len(measures) > 0:
        cols_str = "{" + ", ".join([f"[Measures].[{m}]" for m in measures]) + "}"
    else:
        cols_str = "{[Measures].[Total Item Price]}"
    
    # Build ROWS part
    if dimensions_on_rows and len(dimensions_on_rows) > 0:
        # For nested hierarchies (drill-down)
        if isinstance(dimensions_on_rows, list) and len(dimensions_on_rows) > 0:
            rows_items = []
            for dim in dimensions_on_rows:
                if isinstance(dim, dict):
                    dim_name = dim.get('dimension')
                    level = dim.get('level', '')
                    rows_items.append(f"[{dim_name}].[{level}].MEMBERS")
                else:
                    rows_items.append(f"[{dim}].MEMBERS")
            
            rows_str = "{" + " * ".join(rows_items) + "}"
        else:
            rows_str = "{" + f"[{dimensions_on_rows}].MEMBERS" + "}"
    else:
        rows_str = "{[Dim Item].[Item Description].MEMBERS}"
    
    # Build WHERE clause (filters/slicers)
    where_clause = ""
    if filters and len(filters) > 0:
        filter_items = []
        for f in filters:
            dim = f.get('dimension')
            level = f.get('level')
            value = f.get('value')
            if dim and level and value:
                filter_items.append(f"[{dim}].[{level}].&[{value}]")
        
        if filter_items:
            where_clause = " WHERE (" + ", ".join(filter_items) + ")"
    
    # Final MDX query
    mdx_query = f"""
    SELECT 
        {cols_str} ON COLUMNS,
        {rows_str} ON ROWS
    FROM 
        [DW]
    {where_clause}
    """
    
    logger.debug(f"Generated MDX: {mdx_query}")
    return mdx_query

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/metadata')
def metadata():
    return get_cube_metadata()

@app.route('/api/data')
def get_data():
    try:
        # Simple default query using the actual cube structure
        mdx_query = """
        SELECT 
            {[Measures].[Total Item Price]} ON COLUMNS,
            [Dim Item].[Item Description].MEMBERS ON ROWS
        FROM 
            [DW]
        """
        data = execute_mdx(mdx_query)
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error in get_data: {str(e)}")
        return jsonify({"error": str(e)})

@app.route('/api/query', methods=['POST'])
def execute_query():
    try:
        request_data = request.json
        
        dimensions_on_rows = request_data.get('rows', [])
        dimensions_on_cols = request_data.get('columns', [])
        measures = request_data.get('measures', ['Total Item Price'])
        filters = request_data.get('filters', [])
        
        mdx_query = build_mdx_query(dimensions_on_rows, dimensions_on_cols, measures, filters)
        
        if not mdx_query:
            return jsonify({"error": "Invalid query parameters"})
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in execute_query: {str(e)}")
        return jsonify({"error": str(e)})

@app.route('/api/drill', methods=['POST'])
def drill_down():
    try:
        request_data = request.json
        dimension = request_data.get('dimension')
        current_level = request_data.get('currentLevel')
        drill_to_level = request_data.get('drillToLevel')
        member_value = request_data.get('memberValue')
        measures = request_data.get('measures', ['Total Item Price'])
        
        if not dimension or not current_level or not drill_to_level:
            return jsonify({"error": "Missing required parameters"})
        
        # Build MDX for drill-down
        if member_value:
            mdx_query = f"""
            SELECT 
                {{{", ".join([f"[Measures].[{m}]" for m in measures])}}} ON COLUMNS,
                {{[{dimension}].[{drill_to_level}].MEMBERS}} ON ROWS
            FROM 
                [DW]
            WHERE
                ([{dimension}].[{current_level}].&[{member_value}])
            """
        else:
            mdx_query = f"""
            SELECT 
                {{{", ".join([f"[Measures].[{m}]" for m in measures])}}} ON COLUMNS,
                {{[{dimension}].[{drill_to_level}].MEMBERS}} ON ROWS
            FROM 
                [DW]
            """
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in drill_down: {str(e)}")
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    app.run(debug=True) 