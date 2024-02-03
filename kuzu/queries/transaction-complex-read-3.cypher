OPTIONAL MATCH path = (:Account {id: $id1})-[transfer:AccountTransferAccount* ALL SHORTEST 1..15 (r, n | WHERE $startTime < r.timestamp AND r.timestamp < $endTime)]->(:Account {id: $id2})
RETURN
CASE size(rels(path))
  WHEN 0 THEN -1
  ELSE length(path)
END AS shortestPathLength