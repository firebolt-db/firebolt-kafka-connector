INSERT INTO "target_table_post_processing_file_schemaless" (processed) 
SELECT id::TEXT || UPPER(value)
FROM "post_processing_file_table_schemaless" where batch_id='${firebolt_param.batch_id}';