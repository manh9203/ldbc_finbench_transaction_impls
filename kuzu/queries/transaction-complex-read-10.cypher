MATCH 
  (p1:Person {id: $pid1})-[edge1:PersonInvestCompany]->(m1:Company)
WHERE 
  $startTime < edge1.timestamp AND edge1.timestamp < $endTime
WITH 
  p1,
  count(m1) AS cnt1
MATCH
  (p2:Person {id: $pid2})-[edge2:PersonInvestCompany]->(m2:Company)
WHERE 
  $startTime < edge2.timestamp AND edge2.timestamp < $endTime
WITH
  p1, cnt1,
  p2, count(m2) AS cnt2
MATCH 
  (p1)-[edge3:PersonInvestCompany]->(m12:Company)<-[edge4:PersonInvestCompany]-(p2)
WHERE $startTime < edge3.timestamp AND edge3.timestamp < $endTime
  AND $startTime < edge4.timestamp AND edge4.timestamp < $endTime
WITH
  cnt1, cnt2,
  count(m12) AS cntIntersection
WITH
  cnt1, cnt2, cntIntersection,
  cnt1 + cnt2 - cntIntersection AS cntUnion
WITH
  cntIntersection, cntUnion,
  CASE cntUnion
    WHEN 0 THEN 0.0
    ELSE (1.0 * cntIntersection) / cntUnion
  END AS jaccardSimilarity
RETURN CAST(round(jaccardSimilarity, 3), "FLOAT")
