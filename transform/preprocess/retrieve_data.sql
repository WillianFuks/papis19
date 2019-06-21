SELECT 
  date,
  customer_id,
  --hits
  ARRAY(SELECT AS STRUCT action AS action, productSku AS productSku FROM UNNEST(hits) WHERE action IN ('Browsed', 'AddedToBasket')) AS hits -- cheatting!
FROM `{project_id}.papis19.dafiti_data`
WHERE TRUE
  AND ARRAY_LENGTH(hits) > 1
  AND EXISTS(SELECT 1 FROM UNNEST(hits) WHERE action = 'Browsed')
  AND date BETWEEN '{init_date}' AND '{end_date}'
