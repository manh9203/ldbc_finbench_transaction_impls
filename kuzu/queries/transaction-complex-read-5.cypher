MATCH 
    (person:Person {id: $id})-[:PersonOwnAccount]->(src:Account),
    p = (src)-[transfer:AccountTransferAccount*1..3 (r, n | WHERE $startTime < r.timestamp AND r.timestamp < $endTime)]->(dst)
WITH 
    p AS path,
    properties(rels(p), 'timestamp') AS ts,
    length(p) AS pathLength
WHERE 
    ts = list_sort(ts, 'ASC')
    AND is_acyclic(path)
RETURN path
ORDER BY pathLength DESC
