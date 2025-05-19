from flask import Flask, render_template, jsonify, request, send_from_directory
import sys
import os
import logging
from flask_cors import CORS

"""
OLAP API Documentation

This Flask application provides a backend for OLAP operations on a SSAS cube.
Below are examples of how to use the API endpoints:

1. Store-Product Sales Analysis:
   Endpoint: /api/store_product_sales
   Method: POST
   Payload: {
     "store_id": "1",
     "item_id": "2",
     "time_dimension": "Month",
     "measures": ["Quantity Sale", "Total Item Price"]
   }
   Description: Get sales data for a specific store and product combination over time.

2. Store Time-based Sales Analysis:
   Endpoint: /api/store_time_sales
   Method: POST
   Payload: {
     "store_id": "1",
     "time_level": "Month", // Month, Quarter, or Year
     "year": "2023", // Optional
     "measures": ["Quantity Sale", "Total Item Price"]
   }
   Description: Get total sales for a store across different time periods.

3. Product Time-based Sales Analysis:
   Endpoint: /api/product_time_sales
   Method: POST
   Payload: {
     "item_id": "1",
     "time_level": "Month", // Month, Quarter, or Year
     "year": "2023", // Optional
     "measures": ["Quantity Sale", "Total Item Price"]
   }
   Description: Get sales data for a specific product across time periods.

4. Multi-dimensional Analysis:
   Endpoint: /api/multi_dimension_analysis
   Method: POST
   Payload: {
     "row_dimensions": [
       {"dimension": "Dim Store", "level": "Store ID"}
     ],
     "column_dimensions": [
       {"dimension": "Dim Time", "level": "Month"}
     ],
     "measures": ["Quantity Sale", "Total Item Price"],
     "filters": [
       {"dimension": "Dim Time", "level": "Year", "value": "2023", "type": "equals"}
     ]
   }
   Description: Perform complex multidimensional analysis with multiple dimensions.

5. Time Series Comparison:
   Endpoint: /api/time_series_comparison
   Method: POST
   Payload: {
     "dimension": "Dim Store",
     "dimension_level": "Store ID",
     "time_level": "Month",
     "year_from": "2022",
     "year_to": "2023",
     "measures": ["Quantity Sale", "Total Item Price"]
   }
   Description: Compare sales data across different time periods.

6. Time Hierarchy Analysis:
   Endpoint: /api/time_hierarchy_analysis
   Method: POST
   Payload: {
     "dimension": "Dim Store", 
     "dimension_level": "Store ID",
     "dimension_value": "1", // Optional specific value
     "time_level": "Month", // Day, Month, Quarter, Year
     "year": "2023", // Optional specific year
     "compare_to_previous": true, // Optional - compare with previous period
     "measures": ["Quantity Sale", "Total Item Price"]
   }
   Description: Perform analysis using time hierarchy with automatic aggregation at higher levels,
   optionally comparing current period with previous period.

Example Usage Scenarios:

1. Number of items sold at a specific store for a specific product:
   Use /api/store_product_sales with specific store_id and item_id

2. Total items a store sold in a month, quarter, or year:
   Use /api/store_time_sales with specific store_id and time_level

3. Time-based comparison with automatic hierarchy aggregation:
   Use /api/time_hierarchy_analysis with appropriate parameters
"""

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
CORS(app)

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

# Improved MDX query builder to handle multiple dimensions and advanced aggregations
def build_mdx_query(dimensions_on_rows, dimensions_on_cols, measures, filters=None, drill_info=None, 
                    sort_info=None, top_n=None, show_hide=None, conditional_format=None, aggregation_level=None):
    if not dimensions_on_rows and not dimensions_on_cols:
        return None
    
    # Build COLUMNS part with advanced features
    if dimensions_on_cols and len(dimensions_on_cols) > 0:
        dimension_members = []
        for dim in dimensions_on_cols:
            if isinstance(dim, dict):
                dim_name = dim.get('dimension')
                level = dim.get('level', '')
                
                # Handle drill operations
                if drill_info and drill_info.get('dimension') == dim_name:
                    member = drill_info.get('member')
                    drill_type = drill_info.get('type', 'down')
                    
                    if drill_type == 'down' and member:
                        dimension_members.append(f"[{dim_name}].[{level}].&[{member}].CHILDREN")
                    elif drill_type == 'up' and member:
                        dimension_members.append(f"[{dim_name}].[{level}].&[{member}].PARENT")
                    elif drill_type == 'through':
                        # Drill through to detailed data
                        dimension_members.append(f"[{dim_name}].[{level}].MEMBERS")
                    else:
                        dimension_members.append(f"[{dim_name}].[{level}].MEMBERS")
                else:
                    # Support for specific members
                    specific_member = dim.get('member')
                    if specific_member:
                        dimension_members.append(f"[{dim_name}].[{level}].&[{specific_member}]")
                    else:
                        dimension_members.append(f"[{dim_name}].[{level}].MEMBERS")
        
        # Handle show/hide
        if show_hide and show_hide.get('type') == 'columns':
            visible_members = show_hide.get('members', [])
            if visible_members:
                dimension_members = [f"[{dim_name}].[{level}].&[{member}]" for member in visible_members]
        
        # Crossjoin with measures
        if dimension_members:
            measures_set = "{" + ", ".join([f"[Measures].[{m}]" for m in measures]) + "}"
            if len(dimension_members) == 1:
                cols_str = f"NON EMPTY CROSSJOIN({measures_set}, {dimension_members[0]})"
            else:
                dims_str = "CROSSJOIN(" + ", ".join(dimension_members) + ")"
                cols_str = f"NON EMPTY CROSSJOIN({measures_set}, {dims_str})"
        else:
            cols_str = "{" + ", ".join([f"[Measures].[{m}]" for m in measures]) + "}"
    else:
        cols_str = "{" + ", ".join([f"[Measures].[{m}]" for m in measures]) + "}"
    
    # Build ROWS part with advanced features
    if dimensions_on_rows and len(dimensions_on_rows) > 0:
        dimension_members = []
        for dim in dimensions_on_rows:
            if isinstance(dim, dict):
                dim_name = dim.get('dimension')
                level = dim.get('level', '')
                
                # Handle specific members for precise querying
                specific_member = dim.get('member')
                if specific_member:
                    dimension_members.append(f"[{dim_name}].[{level}].&[{specific_member}]")
                # Handle drill operations
                elif drill_info and drill_info.get('dimension') == dim_name:
                    member = drill_info.get('member')
                    drill_type = drill_info.get('type', 'down')
                    
                    if drill_type == 'down' and member:
                        dimension_members.append(f"[{dim_name}].[{level}].&[{member}].CHILDREN")
                    elif drill_type == 'up' and member:
                        dimension_members.append(f"[{dim_name}].[{level}].&[{member}].PARENT")
                    elif drill_type == 'through':
                        dimension_members.append(f"[{dim_name}].[{level}].MEMBERS")
                    else:
                        dimension_members.append(f"[{dim_name}].[{level}].MEMBERS")
                else:
                    dimension_members.append(f"[{dim_name}].[{level}].MEMBERS")
        
        # Handle show/hide
        if show_hide and show_hide.get('type') == 'rows':
            visible_members = show_hide.get('members', [])
            if visible_members:
                dimension_members = [f"[{dim_name}].[{level}].&[{member}]" for member in visible_members]
        
        if len(dimension_members) == 1:
            rows_str = f"NON EMPTY {{{dimension_members[0]}}}"
        else:
            crossjoin_str = "CROSSJOIN(" + ", ".join(dimension_members) + ")"
            rows_str = f"NON EMPTY {{{crossjoin_str}}}"
        
        # Handle sorting
        if sort_info:
            sort_measure = sort_info.get('measure')
            sort_direction = sort_info.get('direction', 'desc')
            if sort_measure:
                # Fix: Change the order of NON EMPTY and ORDER operations
                # Old problematic code: rows_str = f"ORDER({rows_str}, [Measures].[{sort_measure}], {sort_direction.upper()})"
                # New approach: Apply NON EMPTY after ORDER
                if rows_str.startswith("NON EMPTY"):
                    # Remove the NON EMPTY prefix if it exists
                    inner_set = rows_str.replace("NON EMPTY ", "")
                    rows_str = f"NON EMPTY ORDER({inner_set}, [Measures].[{sort_measure}], {sort_direction.upper()})"
                else:
                    rows_str = f"ORDER({rows_str}, [Measures].[{sort_measure}], {sort_direction.upper()})"
        
        # Handle Top N
        if top_n:
            n = top_n.get('n', 10)
            measure = top_n.get('measure')
            if measure:
                rows_str = f"TOPCOUNT({rows_str}, {n}, [Measures].[{measure}])"
    else:
        rows_str = "{[Measures].MEMBERS}"
    
    # Build WHERE clause with advanced filtering
    where_clause = ""
    if filters and len(filters) > 0:
        filter_items = []
        for f in filters:
            dim = f.get('dimension')
            level = f.get('level')
            value = f.get('value')
            filter_type = f.get('type', 'equals')
            
            if dim and level and value:
                if filter_type == 'equals':
                    filter_items.append(f"[{dim}].[{level}].&[{value}]")
                elif filter_type == 'contains':
                    filter_items.append(f"FILTER([{dim}].[{level}].MEMBERS, CONTAINS([{dim}].[{level}].CURRENTMEMBER.MEMBER_CAPTION, '{value}'))")
                elif filter_type == 'starts_with':
                    filter_items.append(f"FILTER([{dim}].[{level}].MEMBERS, STARTSWITH([{dim}].[{level}].CURRENTMEMBER.MEMBER_CAPTION, '{value}'))")
                elif filter_type == 'ends_with':
                    filter_items.append(f"FILTER([{dim}].[{level}].MEMBERS, ENDSWITH([{dim}].[{level}].CURRENTMEMBER.MEMBER_CAPTION, '{value}'))")
                elif filter_type == 'greater_than':
                    filter_items.append(f"FILTER([{dim}].[{level}].MEMBERS, [{dim}].[{level}].CURRENTMEMBER > {value})")
                elif filter_type == 'less_than':
                    filter_items.append(f"FILTER([{dim}].[{level}].MEMBERS, [{dim}].[{level}].CURRENTMEMBER < {value})")
                elif filter_type == 'between':
                    min_val = f.get('min_value')
                    max_val = f.get('max_value')
                    if min_val and max_val:
                        filter_items.append(f"FILTER([{dim}].[{level}].MEMBERS, [{dim}].[{level}].CURRENTMEMBER >= {min_val} AND [{dim}].[{level}].CURRENTMEMBER <= {max_val})")
        
        if filter_items:
            where_clause = " WHERE (" + ", ".join(filter_items) + ")"
    
    # Handle aggregation levels for time-based aggregations
    if aggregation_level:
        agg_dim = aggregation_level.get('dimension')
        agg_level = aggregation_level.get('level')
        if agg_dim and agg_level:
            # Add to WHERE clause if not already present
            agg_where = f"[{agg_dim}].[{agg_level}].MEMBERS"
            if where_clause:
                # Add to existing WHERE clause
                where_clause = where_clause.replace(" WHERE (", f" WHERE ({agg_where}, ")
            else:
                # Create new WHERE clause
                where_clause = f" WHERE ({agg_where})"
    
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
    return send_from_directory('templates', 'index.html')

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
            [DW]
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
        drill_info = request_data.get('drill_info', None)
        sort_info = request_data.get('sort_info', None)
        top_n = request_data.get('top_n', None)
        show_hide = request_data.get('show_hide', None)
        conditional_format = request_data.get('conditional_format', None)
        aggregation_level = request_data.get('aggregation_level', None)
        
        # Convert simple dimension strings to proper objects if needed
        for i, dim in enumerate(dimensions_on_rows):
            if isinstance(dim, str):
                parts = dim.split('.')
                if len(parts) >= 2:
                    dimensions_on_rows[i] = {
                        'dimension': parts[0],
                        'level': parts[1]
                    }
                    # Check if there's a specific member
                    if len(parts) >= 3:
                        dimensions_on_rows[i]['member'] = parts[2]
        
        for i, dim in enumerate(dimensions_on_cols):
            if isinstance(dim, str):
                parts = dim.split('.')
                if len(parts) >= 2:
                    dimensions_on_cols[i] = {
                        'dimension': parts[0],
                        'level': parts[1]
                    }
                    # Check if there's a specific member
                    if len(parts) >= 3:
                        dimensions_on_cols[i]['member'] = parts[2]
        
        mdx_query = build_mdx_query(
            dimensions_on_rows, 
            dimensions_on_cols, 
            measures, 
            filters, 
            drill_info,
            sort_info,
            top_n,
            show_hide,
            conditional_format,
            aggregation_level
        )
        
        if not mdx_query:
            return jsonify({"error": "Invalid query parameters"})
        
        data = execute_mdx(mdx_query)
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
            [DW]
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
def drill_operation():
    try:
        request_data = request.json
        dimension = request_data.get('dimension')
        current_level = request_data.get('currentLevel')
        target_level = request_data.get('targetLevel')
        member_value = request_data.get('memberValue')
        drill_type = request_data.get('type', 'down')  # down, up, through
        measures = request_data.get('measures', ['Total Item Price'])
        
        if not dimension or not current_level or not target_level:
            return jsonify({"error": "Missing required parameters"})
        
        drill_info = {
            'dimension': dimension,
            'type': drill_type,
            'member': member_value
        }
        
        mdx_query = build_mdx_query(
            [{'dimension': dimension, 'level': target_level}],
            [],
            measures,
            None,
            drill_info
        )
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in drill_operation: {str(e)}")
        return jsonify({"error": str(e)})

# New endpoint for roll-up operation
@app.route('/api/rollup', methods=['POST'])
def roll_up():
    try:
        request_data = request.json
        dimension = request_data.get('dimension')
        current_level = request_data.get('currentLevel')
        rollup_to_level = request_data.get('rollupToLevel')
        member_value = request_data.get('memberValue')
        measures = request_data.get('measures', ['Total Item Price'])
        
        if not dimension or not current_level or not rollup_to_level:
            return jsonify({"error": "Missing required parameters"})
        
        # Build MDX for roll-up
        mdx_query = f"""
        SELECT 
            {{{", ".join([f"[Measures].[{m}]" for m in measures])}}} ON COLUMNS,
            NON EMPTY {{[{dimension}].[{rollup_to_level}].MEMBERS}} ON ROWS
        FROM 
            [DW]
        WHERE
            ([{dimension}].[{current_level}].&[{member_value}])
        """
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in roll_up: {str(e)}")
        return jsonify({"error": str(e)})

# New endpoint for pivot operation
@app.route('/api/pivot', methods=['POST'])
def pivot():
    try:
        request_data = request.json
        dimensions = request_data.get('dimensions', [])
        measures = request_data.get('measures', ['Total Item Price'])
        sort_info = request_data.get('sort_info', None)
        top_n = request_data.get('top_n', None)
        
        if not dimensions or len(dimensions) < 2:
            return jsonify({"error": "Pivot requires at least 2 dimensions"})
        
        # First dimension goes to rows, second to columns
        rows_dim = dimensions[0]
        cols_dim = dimensions[1]
        
        mdx_query = build_mdx_query(
            [rows_dim],
            [cols_dim],
            measures,
            None,
            None,
            sort_info,
            top_n
        )
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in pivot: {str(e)}")
        return jsonify({"error": str(e)})

@app.route('/api/slice', methods=['POST'])
def slice():
    try:
        request_data = request.json
        dimensions = request_data.get('dimensions', [])
        measures = request_data.get('measures', ['Total Item Price'])
        slice_dimension = request_data.get('slice_dimension')
        slice_value = request_data.get('slice_value')
        
        if not dimensions or not slice_dimension or not slice_value:
            return jsonify({"error": "Missing required parameters"})
        
        filters = [{
            'dimension': slice_dimension,
            'level': slice_dimension.split('.')[-1],
            'value': slice_value,
            'type': 'equals'
        }]
        
        mdx_query = build_mdx_query(
            dimensions,
            [],
            measures,
            filters
        )
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in slice: {str(e)}")
        return jsonify({"error": str(e)})

@app.route('/api/top_n', methods=['POST'])
def top_n():
    try:
        request_data = request.json
        dimension = request_data.get('dimension')
        level = request_data.get('level')
        measure = request_data.get('measure', 'Total Item Price')
        n = request_data.get('n', 10)
        
        if not dimension or not level:
            return jsonify({"error": "Missing required parameters"})
        
        top_n_info = {
            'n': n,
            'measure': measure
        }
        
        mdx_query = build_mdx_query(
            [{'dimension': dimension, 'level': level}],
            [],
            [measure],
            None,
            None,
            None,
            top_n_info
        )
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in top_n: {str(e)}")
        return jsonify({"error": str(e)})

@app.route('/api/show_hide', methods=['POST'])
def show_hide():
    try:
        request_data = request.json
        dimension = request_data.get('dimension')
        level = request_data.get('level')
        members = request_data.get('members', [])
        type = request_data.get('type', 'rows')  # rows or columns
        
        if not dimension or not level or not members:
            return jsonify({"error": "Missing required parameters"})
        
        show_hide_info = {
            'type': type,
            'members': members
        }
        
        mdx_query = build_mdx_query(
            [{'dimension': dimension, 'level': level}] if type == 'rows' else [],
            [{'dimension': dimension, 'level': level}] if type == 'columns' else [],
            ['Total Item Price'],
            None,
            None,
            None,
            None,
            show_hide_info
        )
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in show_hide: {str(e)}")
        return jsonify({"error": str(e)})

@app.route('/api/drill-through', methods=['POST'])
def drill_through():
    try:
        request_data = request.json
        row_data = request_data.get('rowData')
        col_data = request_data.get('colData')
        
        if not row_data or not col_data:
            return jsonify({"error": "Missing row or column data"})
        
        # Parse row and column data
        row_parts = row_data.split('|')
        col_parts = col_data.split('|')
        
        # Get current dimensions from request
        dimensions_on_rows = request_data.get('rows', [])
        dimensions_on_cols = request_data.get('columns', [])
        
        # Build WHERE clause for detailed data
        where_items = []
        
        # Add row dimension filters
        for i, dim in enumerate(dimensions_on_rows):
            if i < len(row_parts):
                where_items.append(f"[{dim['dimension']}].[{dim['level']}].&[{row_parts[i]}]")
        
        # Add column dimension filters
        for i, dim in enumerate(dimensions_on_cols):
            if i < len(col_parts):
                where_items.append(f"[{dim['dimension']}].[{dim['level']}].&[{col_parts[i]}]")
        
        where_clause = " WHERE (" + ", ".join(where_items) + ")" if where_items else ""
        
        # Build MDX for detailed data
        mdx_query = f"""
        SELECT 
            {{[Measures].[Total Item Price], [Measures].[Quantity Sale], [Measures].[Quantity Ordered]}} ON COLUMNS,
            NON EMPTY {{[Dim Item].[Item ID].MEMBERS}} ON ROWS
        FROM 
            [DW]
        {where_clause}
        """
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in drill_through: {str(e)}")
        return jsonify({"error": str(e)})

@app.route('/api/store_product_sales', methods=['POST'])
def store_product_sales():
    """Get sales data for a specific store and product combination"""
    try:
        request_data = request.json
        store_id = request_data.get('store_id')
        item_id = request_data.get('item_id')
        time_dimension = request_data.get('time_dimension', 'Month')  # Default to Month level
        
        if not store_id or not item_id:
            return jsonify({"error": "Store ID and Item ID are required"})
        
        # Create dimensions for query
        dimensions_on_rows = [
            {
                'dimension': 'Dim Time',
                'level': time_dimension
            }
        ]
        
        # Create filters
        filters = [
            {
                'dimension': 'Dim Store',
                'level': 'Store ID',
                'value': store_id,
                'type': 'equals'
            },
            {
                'dimension': 'Dim Item',
                'level': 'Item ID',
                'value': item_id,
                'type': 'equals'
            }
        ]
        
        # Select measures
        measures = request_data.get('measures', ['Quantity Sale', 'Total Item Price'])
        
        # Build and execute MDX query
        mdx_query = build_mdx_query(
            dimensions_on_rows,
            [],
            measures,
            filters
        )
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in store_product_sales: {str(e)}")
        return jsonify({"error": str(e)})

@app.route('/api/store_time_sales', methods=['POST'])
def store_time_sales():
    """Get total sales for a store across different time periods"""
    try:
        request_data = request.json
        store_id = request_data.get('store_id')
        time_level = request_data.get('time_level', 'Month')  # Month, Quarter, Year
        year = request_data.get('year')  # Optional year filter
        
        if not store_id:
            return jsonify({"error": "Store ID is required"})
        
        # Create dimensions for query
        dimensions_on_rows = [
            {
                'dimension': 'Dim Time',
                'level': time_level
            }
        ]
        
        # Create filters
        filters = [
            {
                'dimension': 'Dim Store',
                'level': 'Store ID',
                'value': store_id,
                'type': 'equals'
            }
        ]
        
        # Add year filter if provided
        if year:
            filters.append({
                'dimension': 'Dim Time',
                'level': 'Year',
                'value': year,
                'type': 'equals'
            })
        
        # Select measures
        measures = request_data.get('measures', ['Quantity Sale', 'Total Item Price'])
        
        # Sort by time dimension
        sort_info = {
            'measure': 'Total Item Price',
            'direction': 'desc'
        }
        
        # Build and execute MDX query
        mdx_query = build_mdx_query(
            dimensions_on_rows,
            [],
            measures,
            filters,
            None,
            sort_info
        )
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in store_time_sales: {str(e)}")
        return jsonify({"error": str(e)})

@app.route('/api/product_time_sales', methods=['POST'])
def product_time_sales():
    """Get sales data for a specific product across time periods"""
    try:
        request_data = request.json
        item_id = request_data.get('item_id')
        time_level = request_data.get('time_level', 'Month')  # Month, Quarter, Year
        year = request_data.get('year')  # Optional year filter
        
        if not item_id:
            return jsonify({"error": "Item ID is required"})
        
        # Create dimensions for query
        dimensions_on_rows = [
            {
                'dimension': 'Dim Time',
                'level': time_level
            }
        ]
        
        # Create filters
        filters = [
            {
                'dimension': 'Dim Item',
                'level': 'Item ID',
                'value': item_id,
                'type': 'equals'
            }
        ]
        
        # Add year filter if provided
        if year:
            filters.append({
                'dimension': 'Dim Time',
                'level': 'Year',
                'value': year,
                'type': 'equals'
            })
        
        # Select measures
        measures = request_data.get('measures', ['Quantity Sale', 'Total Item Price'])
        
        # Sort by time dimension
        sort_info = {
            'measure': request_data.get('sort_measure', 'Total Item Price'),
            'direction': request_data.get('sort_direction', 'desc')
        }
        
        # Build and execute MDX query
        mdx_query = build_mdx_query(
            dimensions_on_rows,
            [],
            measures,
            filters,
            None,
            sort_info
        )
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in product_time_sales: {str(e)}")
        return jsonify({"error": str(e)})

@app.route('/api/multi_dimension_analysis', methods=['POST'])
def multi_dimension_analysis():
    """Perform a complex multidimensional analysis with multiple dimensions"""
    try:
        request_data = request.json
        row_dimensions = request_data.get('row_dimensions', [])
        column_dimensions = request_data.get('column_dimensions', [])
        measures = request_data.get('measures', ['Quantity Sale', 'Total Item Price'])
        filters = request_data.get('filters', [])
        
        if not row_dimensions and not column_dimensions:
            return jsonify({"error": "At least one dimension must be provided"})
        
        # Build and execute MDX query
        mdx_query = build_mdx_query(
            row_dimensions,
            column_dimensions,
            measures,
            filters
        )
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in multi_dimension_analysis: {str(e)}")
        return jsonify({"error": str(e)})

@app.route('/api/time_series_comparison', methods=['POST'])
def time_series_comparison():
    """Compare sales data across different time periods"""
    try:
        request_data = request.json
        dimension = request_data.get('dimension')  # Store or Item dimension
        dimension_level = request_data.get('dimension_level')  # Store ID, Item ID, etc.
        time_level = request_data.get('time_level', 'Month')
        year_from = request_data.get('year_from')
        year_to = request_data.get('year_to')
        
        if not dimension or not dimension_level:
            return jsonify({"error": "Dimension and dimension level are required"})
        
        # Create dimensions
        dimensions_on_rows = [
            {
                'dimension': dimension,
                'level': dimension_level
            }
        ]
        
        dimensions_on_cols = [
            {
                'dimension': 'Dim Time',
                'level': time_level
            }
        ]
        
        # Create filters for time range
        filters = []
        if year_from and year_to:
            filters.append({
                'dimension': 'Dim Time',
                'level': 'Year',
                'min_value': year_from,
                'max_value': year_to,
                'type': 'between'
            })
        
        # Select measures
        measures = request_data.get('measures', ['Quantity Sale', 'Total Item Price'])
        
        # Build and execute MDX query
        mdx_query = build_mdx_query(
            dimensions_on_rows,
            dimensions_on_cols,
            measures,
            filters
        )
        
        data = execute_mdx(mdx_query)
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in time_series_comparison: {str(e)}")
        return jsonify({"error": str(e)})

@app.route('/api/time_hierarchy_analysis', methods=['POST'])
def time_hierarchy_analysis():
    """Perform analysis using time hierarchy (day -> month -> quarter -> year)"""
    try:
        request_data = request.json
        dimension = request_data.get('dimension')  # Store, Customer, or Item
        dimension_level = request_data.get('dimension_level')  # The level of the dimension to analyze
        dimension_value = request_data.get('dimension_value')  # Optional specific value to filter by
        time_level = request_data.get('time_level', 'Month')  # Day, Month, Quarter, Year
        year = request_data.get('year')  # Optional specific year
        include_current_period = request_data.get('include_current_period', True)  # Include current period data
        compare_to_previous = request_data.get('compare_to_previous', False)  # Compare with previous period
        
        if not dimension or not dimension_level:
            return jsonify({"error": "Dimension and dimension level are required"})
        
        # Create dimensions based on time level
        dimensions_on_rows = [
            {
                'dimension': 'Dim Time',
                'level': time_level
            }
        ]
        
        # If comparing with a specific dimension (like Store)
        if dimension != 'Dim Time':
            dimensions_on_cols = [
                {
                    'dimension': dimension,
                    'level': dimension_level
                }
            ]
        else:
            dimensions_on_cols = []
        
        # Create filters
        filters = []
        
        # Filter by dimension value if provided
        if dimension_value and dimension != 'Dim Time':
            filters.append({
                'dimension': dimension,
                'level': dimension_level,
                'value': dimension_value,
                'type': 'equals'
            })
        
        # Filter by year if provided
        if year:
            filters.append({
                'dimension': 'Dim Time',
                'level': 'Year',
                'value': year,
                'type': 'equals'
            })
        
        # Select measures
        measures = request_data.get('measures', ['Quantity Sale', 'Total Item Price'])
        
        # Set up sort order for time dimension (typically ascending for time)
        sort_info = {
            'measure': measures[0],
            'direction': 'asc'
        }
        
        # Additional time-based aggregation information
        aggregation_level = None
        if time_level != 'Year':  # Only needed for levels below Year
            parent_level = None
            if time_level == 'Day':
                parent_level = 'Month'
            elif time_level == 'Month':
                parent_level = 'Quarter'
            elif time_level == 'Quarter':
                parent_level = 'Year'
            
            if parent_level:
                aggregation_level = {
                    'dimension': 'Dim Time',
                    'level': parent_level
                }
        
        # Build and execute MDX query
        mdx_query = build_mdx_query(
            dimensions_on_rows,
            dimensions_on_cols,
            measures,
            filters,
            None,
            sort_info,
            None,
            None,
            None,
            aggregation_level
        )
        
        data = execute_mdx(mdx_query)
        
        # If requested to compare with previous period, get that data too
        if compare_to_previous and year:
            # Calculate previous year
            prev_year = str(int(year) - 1)
            
            # Update filter for previous year
            prev_filters = [f for f in filters if f.get('dimension') != 'Dim Time' or f.get('level') != 'Year']
            prev_filters.append({
                'dimension': 'Dim Time',
                'level': 'Year',
                'value': prev_year,
                'type': 'equals'
            })
            
            # Build and execute MDX query for previous period
            prev_mdx_query = build_mdx_query(
                dimensions_on_rows,
                dimensions_on_cols,
                measures,
                prev_filters,
                None,
                sort_info,
                None,
                None,
                None,
                aggregation_level
            )
            
            prev_data = execute_mdx(prev_mdx_query)
            
            return jsonify({
                "current_data": data, 
                "previous_data": prev_data, 
                "current_mdx": mdx_query,
                "previous_mdx": prev_mdx_query
            })
        
        return jsonify({"data": data, "mdx": mdx_query})
    except Exception as e:
        logger.error(f"Error in time_hierarchy_analysis: {str(e)}")
        return jsonify({"error": str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)