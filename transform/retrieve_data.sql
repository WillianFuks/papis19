SELECT 
  date,
  customer_id,
  hits
FROM `{project_id}.papis19.test1`
WHERE ARRAY_LENGTH(hits) > 1
LIMIT 2
