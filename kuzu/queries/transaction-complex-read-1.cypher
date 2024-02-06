MATCH
  p = (account:Account {id: $id})-[edge1:AccountTransferAccount*1..3 (r, n | WHERE $startTime < r.timestamp AND r.timestamp < $endTime)]->(other:Account),
  (other)<-[edge2:MediumSignInAccount]-(medium:Medium {isBlocked: true})
WITH
  edge2.timestamp AS signInTime,
  properties(rels(p), 'timestamp') AS ts,
  length(p) AS pathLength,
  other, medium
WHERE 
  $startTime < signInTime AND signInTime < $endTime
  AND ts = list_sort(ts, 'ASC')
WITH
  other.id AS otherId, pathLength AS accountDistance, medium.id AS mediumId, medium.type AS mediumType
RETURN otherId, CAST(accountDistance, "INT32"), mediumId, mediumType
ORDER BY accountDistance, otherId, mediumId