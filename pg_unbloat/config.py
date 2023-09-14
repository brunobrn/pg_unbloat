import sys,argparse
from getpass import getpass

# The application name to identify the session on PostgreSQL
app_name="pg_unbloat"

# Configuration of parameters to connect on the instance
desc = "This automation identify bloated tables in a especific database, it's not possible run this automation in a read replica and the user needs to have superuser grants"
 
parser = argparse.ArgumentParser(description = desc)

parser.add_argument("--host", help="Host address to access the database. Ex: 127.0.0.1", required=True)
parser.add_argument("--dbname", help="Name of the database to check tables, yes we can execute only in one database at a time", required=True)
parser.add_argument("--user", help="Name of user to connect in the database, the user need to have superuser grants", required=True)
parser.add_argument("--port", help="Port of to connect on instance", default=5432)
parser.add_argument("--table_min_size", help="Minimum size to scan tables", default=1)
parser.add_argument("--table_max_size", help="Maximum size to scan tables, when bigger the value more time and I/O the automation will use.", default=2048)

args            = parser.parse_args()
host            = args.__dict__["host"]
dbname          = args.__dict__["dbname"]
user            = args.__dict__["user"]
port            = args.__dict__["port"]
table_min_size  = args.__dict__["table_min_size"]
table_max_size  = args.__dict__["table_max_size"]
password        = getpass()
