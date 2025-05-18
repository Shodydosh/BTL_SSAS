from flask import Flask, render_template, jsonify, request
import sys
import os
import logging
from flask_cors import CORS

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Try multiple potential ADOMD.NET paths
adomd_paths = [
    r'C:\Program Files\Microsoft.NET\ADOMD.NET\150',
    r'C:\Program Files (x86)\Microsoft.NET\ADOMD.NET\150',
    r'C:\Program Files\Microsoft Analysis Services\ADOMD\150',
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
CORS(app)

# SSAS connection parameters
model_name = 'CUBE_STORE_IMPORT'
database_name = 'MultidimensionalProject2'
server_name = r'26.161.151.19'

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
                
                # Better handling of null values
                # For dimension members (usually strings), keep null for aggregation rows
                # For measures (usually numeric), replace NaN with 0 for better visualization
                for col in df.columns:
                    if '[Measures]' in col:
                        # Replace NaN in measures with 0 instead of None
                        df[col] = df[col].fillna(0)
                    else:
                        # For dimensions that have None as the first row (often the total)
                        # We'll label it as "Total" for better clarity
                        if col.endswith('.[MEMBER_CAPTION]') and df[col].iloc[0] is None:
                            df[col].iloc[0] = 'Total'
                        
                        # Replace "Unknown" values with more meaningful labels
                        df[col] = df[col].apply(lambda x: 'Total' if x == 'Unknown' else x)
                
                # Filter out rows where all dimension columns have "Unknown" values
                if len(df) > 1:  # Keep at least one row (likely the total)
                    dimension_cols = [col for col in df.columns if '[Measures]' not in col]
                    if dimension_cols:
                        # Create a mask for rows where all dimension values are Unknown
                        unknown_mask = df[dimension_cols].apply(lambda row: all(val == 'Unknown' for val in row), axis=1)
                        # Only drop unknown rows if we have more than one row left after filtering
                        if sum(~unknown_mask) > 0:
                            df = df[~unknown_mask]
                
                result = df.to_dict('records')
                return result
    except Exception as e:
        logger.error(f"Error executing MDX: {str(e)}")
        return {"error": str(e)}

# Get cube metadata (dimensions and measures)
def get_cube_metadata():
    try:
        # Updated with correct hierarchies based on user input
        dimensions = {
            "Dim Time": ["Day", "Month", "Quarter", "Year"],  # Bottom-up hierarchy
            "Dim Store": ["Store ID", "City ID", "State"],    # Geographic hierarchy
            "Dim Customer": ["Customer ID"],                  # No hierarchy, just ID
            "Dim Item": ["Item ID"]                           # No hierarchy, just ID
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
        # For PowerBI-like multi-dimensional analysis
        if isinstance(dimensions_on_rows, list) and len(dimensions_on_rows) > 0:
            if len(dimensions_on_rows) == 1:
                # Single dimension case
                dim = dimensions_on_rows[0]
                if isinstance(dim, dict):
                    dim_name = dim.get('dimension')
                    level = dim.get('level', '')
                    rows_str = f"NON EMPTY {{[{dim_name}].[{level}].MEMBERS}}"
                else:
                    # Handle string case with default level
                    default_levels = {
                        "Dim Store": "Store ID",
                        "Dim Time": "Year",
                        "Dim Customer": "Customer ID",
                        "Dim Item": "Item ID"
                    }
                    level = default_levels.get(dim, "")
                    rows_str = f"NON EMPTY {{[{dim}].[{level}].MEMBERS}}"
            else:
                # Multiple dimensions - create crossjoin
                dimension_members = []
                for dim in dimensions_on_rows:
                    if isinstance(dim, dict):
                        dim_name = dim.get('dimension')
                        level = dim.get('level', '')
                        dimension_members.append(f"[{dim_name}].[{level}].MEMBERS")
                
                # Use CROSSJOIN for multiple dimensions
                crossjoin_str = "CROSSJOIN(" + ", ".join(dimension_members) + ")"
                rows_str = f"NON EMPTY {{{crossjoin_str}}}"
        else:
            # Handle string case with default level
            if isinstance(dimensions_on_rows, str):
                default_levels = {
                    "Dim Store": "Store ID",
                    "Dim Time": "Year",
                    "Dim Customer": "Customer ID",
                    "Dim Item": "Item ID"
                }
                level = default_levels.get(dimensions_on_rows, "")
                rows_str = f"NON EMPTY {{[{dimensions_on_rows}].[{level}].MEMBERS}}"
            else:
                rows_str = f"NON EMPTY {{[{dimensions_on_rows}].MEMBERS}}"
    else:
        rows_str = "NON EMPTY {[Dim Item].[Item Description].MEMBERS}"
    
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
        [CUBE_STORE_IMPORT]
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
        # Get the return_total_only parameter from query string
        return_total_only = request.args.get('return_total_only', 'false').lower() == 'true'
        
        # Simple default query using the actual cube structure
        mdx_query = """
        SELECT 
            {[Measures].[Total Item Price]} ON COLUMNS,
            NON EMPTY [Dim Item].[Item ID].MEMBERS ON ROWS
        FROM 
            [CUBE_STORE_IMPORT]
        """
        data = execute_mdx(mdx_query)
        
        if return_total_only:
            # Extract and return only the total value
            totals = {}
            if data and len(data) > 0:
                measure_col = '[Measures].[Total Item Price]'
                if measure_col in data[0]:
                    # First row typically contains the total
                    totals['Total Item Price'] = data[0][measure_col]
                else:
                    # Calculate total by summing all values
                    totals['Total Item Price'] = sum(row[measure_col] for row in data if measure_col in row)
            return jsonify({"totals": totals, "mdx": mdx_query})
        
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
        return_total_only = request_data.get('return_total_only', False)
        
        mdx_query = build_mdx_query(dimensions_on_rows, dimensions_on_cols, measures, filters)
        
        if not mdx_query:
            return jsonify({"error": "Invalid query parameters"})
        
        data = execute_mdx(mdx_query)
        
        if return_total_only:
            # Extract and return only the total values for each measure
            totals = {}
            if data and len(data) > 0:
                for measure in measures:
                    measure_col = f'[Measures].[{measure}]'
                    if measure_col in data[0]:
                        # First row typically contains the total for the entire dataset
                        totals[measure] = data[0][measure_col]
                    else:
                        # If total not in first row, calculate it by summing all values
                        totals[measure] = sum(row[measure_col] for row in data if measure_col in row)
            return jsonify({"totals": totals, "mdx": mdx_query})
        
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in execute_query: {str(e)}")
        return jsonify({"error": str(e)})

# New endpoint to get dimension members for filtering
@app.route('/api/members', methods=['GET'])
def get_dimension_members():
    try:
        dimension = request.args.get('dimension')
        level = request.args.get('level')
        
        if not dimension or not level:
            return jsonify({"error": "Missing dimension or level parameter"})
        
        # Build MDX to get members of a dimension level
        mdx_query = f"""
        SELECT 
            {{[Measures].[Total Item Price]}} ON COLUMNS,
            NON EMPTY {{[{dimension}].[{level}].MEMBERS}} ON ROWS
        FROM 
            [CUBE_STORE_IMPORT]
        """
        
        result = execute_mdx(mdx_query)
        
        # Extract member values from result
        members = []
        member_caption_col = f'[{dimension}].[{level}].[MEMBER_CAPTION]'
        
        for row in result:
            if member_caption_col in row and row[member_caption_col] is not None:
                members.append(row[member_caption_col])
        
        return jsonify({"members": members, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error getting dimension members: {str(e)}")
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
        return_total_only = request_data.get('return_total_only', False)
        
        if not dimension or not current_level or not drill_to_level:
            return jsonify({"error": "Missing required parameters"})
        
        # Build MDX for drill-down
        if member_value:
            mdx_query = f"""
            SELECT 
                {{{", ".join([f"[Measures].[{m}]" for m in measures])}}} ON COLUMNS,
                NON EMPTY {{[{dimension}].[{drill_to_level}].MEMBERS}} ON ROWS
            FROM 
                [CUBE_STORE_IMPORT]
            WHERE
                ([{dimension}].[{current_level}].&[{member_value}])
            """
        else:
            mdx_query = f"""
            SELECT 
                {{{", ".join([f"[Measures].[{m}]" for m in measures])}}} ON COLUMNS,
                NON EMPTY {{[{dimension}].[{drill_to_level}].MEMBERS}} ON ROWS
            FROM 
                [CUBE_STORE_IMPORT]
            """
        
        data = execute_mdx(mdx_query)
        
        if return_total_only:
            # Extract and return only the total values for each measure
            totals = {}
            if data and len(data) > 0:
                for measure in measures:
                    measure_col = f'[Measures].[{measure}]'
                    if measure_col in data[0]:
                        # First row typically contains the total
                        totals[measure] = data[0][measure_col]
                    else:
                        # Calculate total by summing all values
                        totals[measure] = sum(row[measure_col] for row in data if measure_col in row)
            return jsonify({"totals": totals, "mdx": mdx_query})
        
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in drill_down: {str(e)}")
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    app.run(debug=True) 