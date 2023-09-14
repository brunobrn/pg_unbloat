from config import app_name

GET_SCHEMAS = ('''SELECT n.nspname AS "Name"
                            FROM pg_catalog.pg_namespace n
                                WHERE n.nspname !~ '^pg_' 
                           AND n.nspname not in ('information_schema','repack');''') 

GET_TABLES =    ("""
                SELECT n.nspname,c.relname as "Name"
                    , pg_catalog.pg_table_size(c.oid)/1024/1024 as sizeMB
                    FROM pg_catalog.pg_class c
                        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
                    WHERE c.relkind IN ('r','')
                        AND n.nspname <> 'pg_catalog'
                        AND n.nspname !~ '^pg_toast'
                        AND n.nspname <> 'information_schema'
                        AND c.relpersistence = 'p'
                        AND c.relname not like '%_p%_w%'
                    AND pg_catalog.pg_table_is_visible(c.oid)
                    ORDER BY 1;
                """) 
