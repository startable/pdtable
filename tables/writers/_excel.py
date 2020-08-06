try: 
    import openpyxl
except ImportError as err:
    raise ImportError(
        "Unable to find a usable spreadsheet engine. "
        "Tried using: 'openpyxl'.\n"
        "Please install openpyxl for Excel I/O support."
        )


