SELECT 
  SHA256(fullvisitorid) AS customer_id,
  PARSE_DATE("%Y%m%d", date) AS date,
  STRUCT(
    SHA256(CONCAT(COALESCE(trafficSource.adContent, ''), CAST(RAND() AS STRING))) AS adContent,
    SHA256(CONCAT(COALESCE(trafficSource.adwordsClickInfo.adNetworkType, ''), CAST(RAND() AS STRING))) AS adNetworkType,
    SHA256(CONCAT(COALESCE(trafficSource.adwordsClickInfo.criteriaParameters, ''), CAST(RAND() AS STRING))) AS criteriaParameters,
    SHA256(CONCAT(COALESCE(trafficSource.adwordsClickInfo.gclId, ''), CAST(RAND() AS STRING))) AS gclId,
    SHA256(CONCAT(COALESCE(trafficSource.adwordsClickInfo.slot, ''), CAST(RAND() AS STRING))) AS slot,
    SHA256(CONCAT(COALESCE(trafficSource.campaign, ''), CAST(RAND() AS STRING))) AS campaign,
    SHA256(CONCAT(COALESCE(trafficSource.campaignCode, ''), CAST(RAND() AS STRING))) AS campaignCode
  ) AS traffic,
  SHA256(CONCAT(COALESCE(channelGrouping, ''), CAST(RAND() AS STRING))) AS channelGrouping,
  STRUCT(
    SHA256(CONCAT(COALESCE(device.browser, ''), CAST(RAND() AS STRING))) AS browser,
    SHA256(CONCAT(COALESCE(device.browserSize, ''), CAST(RAND() AS STRING))) AS size,
    SHA256(CONCAT(COALESCE(device.browserVersion, ''), CAST(RAND() AS STRING))) AS version
  ) AS device,
  ARRAY(
    SELECT AS STRUCT
      hitNumber,
      SHA256(CONCAT(COALESCE(sourcePropertyInfo.sourcePropertyDisplayName, ''), CAST(RAND() AS STRING))) AS sourcePropertyDisplayName,
      productSku,
      CASE WHEN eCommerceAction.action_type = '2' THEN 'Browsed'
           WHEN eCommerceAction.action_type = '3' THEN 'AddedToCart' END AS action
    FROM UNNEST(hits), UNNEST(product)
    WHERE TRUE
  ) AS hits
FROM `{project_id}.{dataset_id}.ga_sessions_*`
WHERE TRUE
  AND _TABLE_SUFFIX BETWEEN '{init_date}' AND 'end_date'
  AND RAND() < {threshold}
