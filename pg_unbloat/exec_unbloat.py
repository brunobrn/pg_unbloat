from unbloat_functions import get_tables,exec_stattuple_approx,exec_stattuple,init_migration,clean_migration,ddl_creator,create_copy_table,drop_copy_table,get_table_size
from config import table_min_size,table_max_size

tables_to_check = get_tables()

tables_to_run_stattuple = []
tables_without_data = []
tables_already_ok = []

# Eleging tables to full statspack
print("Running parcial stattuple on all tables between %s MB and %s MB \n#################" % (table_min_size,table_max_size))
for each in tables_to_check:
    print("Running on: %s" % (each))
    tuple_percent_approx = [exec_stattuple_approx(each)]
    if tuple_percent_approx[0][0][4] < 1:
        tables_without_data.append(each)
    elif tuple_percent_approx[0][0][4] <60:
        tables_to_run_stattuple.append(each)
    else:
        tables_already_ok.append(each)
print("#################\nParcial stattuple finished sucessfully!")

# Filtering more bloated tables to scan
if len(tables_to_run_stattuple) >= 1:
    print("This Tables are candidate to a full stattuple:", tables_to_run_stattuple)
    init_migration()
else:
    print("#################\nNo tables to do a full analyse")    

for each in tables_to_run_stattuple:
    print("#################\nRunning full stattuple on %s:" % (each))
    tuple_percent = [exec_stattuple(each)]
    print("The tuple percentage of table %s is: %s" % (each,tuple_percent[0][0][3]))

    # Executing the full check in more bloated tables
    if tuple_percent[0][0][3] < 50:
        print("The real percent of tuple is less than 50, checking the real bloat in %s" % (each))
        create_copy_table(ddl_creator(each)[0])

        main_table_name_size_bytes = get_table_size(each)
        print("#################\nSize of main table in bytes: %s" % (main_table_name_size_bytes))
        copied_table_name = ddl_creator(each)[1]
        copied_table_name_size_bytes = get_table_size(copied_table_name)
        print("Size of copied table in bytes: %s" % (copied_table_name_size_bytes))
        diff_percent = (main_table_name_size_bytes - copied_table_name_size_bytes) / main_table_name_size_bytes*100
        print("The copied is %s percent litgher than the main table size" % (diff_percent))

        if diff_percent >= 80:
            print("The table %s are high bloated and the queries will be use a lot of CPU" % (main_table_name_size_bytes))
        if diff_percent >= 50 <= 79:
            print("The table %s are elegible to a repack" % (main_table_name_size_bytes))
        else:
            print("The table %s are ok, a repack here are not urgent" % (main_table_name_size_bytes))

        drop_copy_table(copied_table_name)

clean_migration()
