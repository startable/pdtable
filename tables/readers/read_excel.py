from tables.store import StarBlockType, BlockGenerator


try:
    import openpyxl
    try:
        from openpyxl.worksheet.worksheet import Worksheet as OpenpyxlWorksheet
    except ImportError:
        # openpyxl < 2.6
        from openpyxl.worksheet import Worksheet as OpenpyxlWorksheet    
except ImportError as err:
    raise ImportError(
        "Unable to find a usable Excel engine. "
        "Tried using: 'openpyxl'.\n"
        "Please install openpyxl for Excel I/O support."
    )


def _blocks(ws: OpenpyxlWorksheet) -> BlockGenerator:
    
    block_rows = []
    block_type = StarBlockType.METADATA
    block_start_row = 0
    for irow_0based, row in enumerate(ws.iter_rows(values_only=True)):
        #TODO iterate on cells instead of rows? because all rows are as wide as the rightmost thing in the sheet
        next_block = None
        first_cell = row[0]
        first_cell_is_str = isinstance(first_cell, str)
        if first_cell_is_str:
            if first_cell.startswith("**"):
                if first_cell.startswith("***"):
                    next_block = StarBlockType.DIRECTIVE
                else:
                    next_block = StarBlockType.TABLE
            elif first_cell.startswith(":"):
                next_block = StarBlockType.TEMPLATE_ROW
        elif (first_cell is None or (first_cell_is_str and first_cell == "")) and not block_type == StarBlockType.METADATA:
            next_block = StarBlockType.BLANK
        
        if next_block is not None:
            yield block_rows  #TODO replace with make_token
            block_rows = []
            block_start_row = irow_0based + 1
        block_rows.append(row)

    if block_rows:
        yield block_rows  #TODO replace with make_token
    

        
