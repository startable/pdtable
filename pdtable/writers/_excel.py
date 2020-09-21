"""Interface to write Tables to an Excel workbook.

The only Excel I/O engine supported right now is 'openpyxl', but this module can
be extended to support others such as 'xlsxwriter'. 

openpyxl (and eventually other engines) are not required at install time; 
only when write_excel() is called for the first time. 
"""
