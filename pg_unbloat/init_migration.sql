-- FUNCTION: public.get_storage_param(oid)

-- DROP FUNCTION IF EXISTS public.get_storage_param(oid);

CREATE OR REPLACE FUNCTION public.get_storage_param(
	oid)
    RETURNS text
    LANGUAGE 'sql'
    COST 100
    STABLE STRICT PARALLEL UNSAFE
AS $BODY$
SELECT string_agg(param, ', ')
FROM (
    -- table storage parameter
    SELECT unnest(reloptions) as param
    FROM pg_class
    WHERE oid = $1
    UNION ALL
    -- TOAST table storage parameter
    SELECT ('toast.' || unnest(reloptions)) as param
    FROM (
        SELECT reltoastrelid from pg_class where oid = $1
         ) as t,
        pg_class as c
    WHERE c.oid = t.reltoastrelid
    UNION ALL
    -- table oid
    SELECT 'oids = ' ||
        CASE WHEN false
            THEN 'true'
            ELSE 'false'
        END
    FROM pg_class
    WHERE oid = $1

    ) as t
$BODY$;

-- FUNCTION: public.get_columns_for_create_as(oid)

-- DROP FUNCTION IF EXISTS public.get_columns_for_create_as(oid);

CREATE OR REPLACE FUNCTION public.get_columns_for_create_as(
	oid)
    RETURNS text
    LANGUAGE 'sql'
    COST 100
    STABLE STRICT PARALLEL UNSAFE
AS $BODY$
SELECT coalesce(string_agg(c, ','), '') FROM (SELECT
	CASE WHEN attisdropped
		THEN 'NULL::integer AS ' || quote_ident(attname)
		ELSE quote_ident(attname)
	END AS c
FROM pg_attribute
WHERE attrelid = $1 AND attnum > 0 ORDER BY attnum
) AS COL
$BODY$;

-- FUNCTION: public.oid2text(oid)

-- DROP FUNCTION IF EXISTS public.oid2text(oid);

CREATE OR REPLACE FUNCTION public.oid2text(
	oid)
    RETURNS text
    LANGUAGE 'sql'
    COST 100
    STABLE STRICT PARALLEL UNSAFE
    SET search_path=pg_catalog
AS $BODY$
	SELECT textin(regclassout($1));
$BODY$;

-- FUNCTION: public.get_ddl(text)

-- DROP FUNCTION IF EXISTS public.get_ddl(text);


 CREATE OR REPLACE FUNCTION public.get_ddl(text) 
     RETURNS TABLE ( create_table_1 text,
					create_table_2 text ,
					create_table_3 text ,
					created_table_name text
	 )
    LANGUAGE 'sql'
	AS $BODY$
    select  
    ((('CREATE TABLE public.table_bloat_'::text || r.oid) || ' WITH ('::text) || repack.get_storage_param(r.oid)) || ') TABLESPACE '::text AS create_table_1,
    COALESCE(quote_ident(s.spcname::text), 'pg_default'::text) AS create_table_2,
    ((' AS SELECT '::text || repack.get_columns_for_create_as(r.oid)) || ' FROM ONLY '::text) || repack.oid2text(r.oid) AS create_table_3,
    'public.table_bloat_'::text || r.oid AS created_table_name
	   FROM pg_class r
     LEFT JOIN pg_class t ON r.reltoastrelid = t.oid
     LEFT JOIN repack.primary_keys pk ON r.oid = pk.indrelid
     LEFT JOIN ( SELECT cki.indexrelid,
            cki.indrelid,
            cki.indnatts,
            cki.indnkeyatts,
            cki.indisunique,
            cki.indisprimary,
            cki.indisexclusion,
            cki.indimmediate,
            cki.indisclustered,
            cki.indisvalid,
            cki.indcheckxmin,
            cki.indisready,
            cki.indislive,
            cki.indisreplident,
            cki.indkey,
            cki.indcollation,
            cki.indclass,
            cki.indoption,
            cki.indexprs,
            cki.indpred
           FROM pg_index cki,
            pg_class ckt
          WHERE cki.indisvalid AND cki.indexrelid = ckt.oid AND cki.indisclustered AND ckt.relam = 403::oid) ck ON r.oid = ck.indrelid
     LEFT JOIN pg_namespace n ON n.oid = r.relnamespace
     LEFT JOIN pg_tablespace s ON s.oid = r.reltablespace
  WHERE r.relkind = 'r'::"char" AND r.relpersistence = 'p'::"char" AND (n.nspname <> ALL (ARRAY['pg_catalog'::name, 'information_schema'::name])) AND n.nspname !~~ 'pg\_temp\_%'::text
  and repack.oid2text(r.oid) = $1
  $BODY$;
  
-- FUNCTION: public.get_table_size(text)

-- DROP FUNCTION IF EXISTS public.get_table_size(text);

CREATE OR REPLACE FUNCTION public.get_table_size(text) 
     RETURNS TABLE ( SIZE_IN_BYTES int
	 )
    LANGUAGE 'sql'
	AS $BODY$
SELECT PG_CATALOG.PG_TABLE_SIZE(C.OID) AS SIZE_IN_BYTES
                    FROM PG_CATALOG.PG_CLASS C
                    LEFT JOIN PG_CATALOG.PG_NAMESPACE N ON N.OID = C.RELNAMESPACE
                    LEFT JOIN PG_CATALOG.PG_AM AM ON AM.OID = C.RELAM
                    WHERE C.RELKIND IN ('r','p','v','m','S','f','')
                        AND N.NSPNAME <> 'pg_catalog'
                        AND N.NSPNAME !~ '^pg_toast'
                        AND N.NSPNAME <> 'information_schema'
                        AND PG_CATALOG.PG_TABLE_IS_VISIBLE(C.OID)
	                    AND C.RELNAME = $1
  $BODY$;
  
-- EXTENSION: PGSTATTUPLE;

-- DROP EXTENSION: DROP EXTENSION IF EXISTS PGSTATTUPLE;

CREATE EXTENSION IF NOT EXISTS PGSTATTUPLE;
