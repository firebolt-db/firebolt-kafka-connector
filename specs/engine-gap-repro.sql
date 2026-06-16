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
-- NOT a gap — double -> real works (this corrects an earlier mistaken claim).
-- read_json types JSON numbers as double; assigning to a REAL (float4) column succeeds for all
-- in-range values and is rejected ONLY when the magnitude overflows float4 (~3.4e38). Verified:
--   12.34, -12345.67, 1234567.89, 0.0000123456, 1.23e6, 1.4e-45  -> all OK
--   3.4028235e38 (Float.MAX_VALUE)                               -> FAIL ("cannot be safely converted")
-- So RealSchemalessSerializerTest fails only on its Float.MAX_VALUE / -Float.MAX_VALUE edge records,
-- not on normal data — a test edge, not a missing cast.
----------------------------------------------------------------------------------------------------
