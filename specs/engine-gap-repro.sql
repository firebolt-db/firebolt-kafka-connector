-- Reproduces the assignment-cast gaps that keep the Kafka connector's serialization
-- integration tests red. The connector ingests with
--     INSERT INTO <table> (<cols>) SELECT <fields> FROM read_json|read_parquet('upload://batch')
-- so the failing operation is an ASSIGNMENT cast: read_xxx surfaces a field as text / bigint /
-- struct, and assigning it to the typed column is rejected. Each query below is that same
-- assignment in miniature. Verified on engine release-5.0.1-0.20260615205019.4f5c69172cd3.
--
-- Run top to bottom in any Firebolt SQL client. The "GAP" statements error; the "OK" ones succeed.

CREATE TABLE assign_probe (
    n     NUMERIC(38,9),
    b     BOOLEAN,
    by    BYTEA,
    ts    TIMESTAMP,
    tstz  TIMESTAMPTZ,
    dt    DATE,
    j     JSON,
    s     STRUCT(k TEXT)
);

----------------------------------------------------------------------------------------------------
-- GAP 1 — text -> numeric / boolean / bytea
-- Hit by every *FromString / *AsString test column (bigDecimalFromString, booleanFromString,
-- byteaAsString, ...). The JSON field is a string, the column is typed.
----------------------------------------------------------------------------------------------------
INSERT INTO assign_probe (n)  SELECT '42.42'::text;       -- ERROR: text can't be assigned to column n of the type numeric(38, 9)
INSERT INTO assign_probe (b)  SELECT 'true'::text;        -- ERROR: text can't be assigned to column b of the type boolean
INSERT INTO assign_probe (by) SELECT 'DEADBEEF'::text;    -- ERROR: text can't be assigned to column by of the type bytea

----------------------------------------------------------------------------------------------------
-- GAP 2 — integer epoch -> date / timestamp / timestamptz
-- read_parquet surfaces Parquet TIMESTAMP logical types as bigint (DATE logical works!), and
-- schemaless JSON epochs are numbers. Assigning the integer to a temporal column is rejected.
----------------------------------------------------------------------------------------------------
INSERT INTO assign_probe (ts)   SELECT 1718000000000::bigint;  -- ERROR: bigint can't be assigned to column ts of the type timestamp
INSERT INTO assign_probe (tstz) SELECT 1718000000000::bigint;  -- ERROR: bigint can't be assigned to column tstz of the type timestamptz
INSERT INTO assign_probe (dt)   SELECT 19737::bigint;          -- ERROR: bigint can't be assigned to column dt of the type date

----------------------------------------------------------------------------------------------------
-- GAP 3 — struct -> json
-- A nested object/record (read_xxx surfaces it as a STRUCT) into a JSON column. (Type-checks at
-- plan time, so it errors even though assign_probe is empty.)
----------------------------------------------------------------------------------------------------
INSERT INTO assign_probe (j) SELECT s FROM assign_probe;       -- ERROR: struct("k" text) can't be assigned to column j of the type json

----------------------------------------------------------------------------------------------------
-- CONTRAST — these ALREADY succeed, so the connector handles them today:
----------------------------------------------------------------------------------------------------
INSERT INTO assign_probe (n)   SELECT 42.42;                     -- OK  numeric literal  -> numeric
INSERT INTO assign_probe (ts)  SELECT '2024-06-10 06:13:20'::text; -- OK  ISO string     -> timestamp
INSERT INTO assign_probe (dt)  SELECT '2024-01-15'::text;        -- OK  ISO string       -> date
INSERT INTO assign_probe (j)   SELECT '{"k":"v"}'::text;         -- OK  JSON string      -> json
INSERT INTO assign_probe (b)   SELECT TRUE;                      -- OK  bool literal     -> boolean
-- text -> double / real / integer / bigint also already work (only numeric/boolean/bytea are gaps).

----------------------------------------------------------------------------------------------------
-- GAP 4 — double -> real (value-dependent, runtime only)
-- Pure-literal casts get folded and pass, so this one only shows through read_json's *runtime*
-- double. Run via the HTTP upload:// API (REAL column fed a JSON number):
--
--   printf '{"r":12.34}\n' > /tmp/r.ndjson
--   curl -sS --form "sql=CREATE TABLE rt (r REAL)" <engine-url>
--   curl -sS --form "sql=INSERT INTO rt SELECT r FROM read_json('upload://d')" --form "d=@/tmp/r.ndjson" <engine-url>
--   -- ERROR: Value of type double precision cannot be safely converted into type real
----------------------------------------------------------------------------------------------------
