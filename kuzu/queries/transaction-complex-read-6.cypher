MATCH 
    (dstCard:Account {id: $id})<-[transfer2:AccountWithdrawAccount]-(mid:Account)<-[transfer1:AccountTransferAccount]-(:Account)
WHERE 
    dstCard.type = "debit card" OR dstCard.type = "credit card" OR dstCard.type = "prepaid card"
    AND $startTime < transfer2.timestamp AND transfer2.timestamp < $endTime 
    AND transfer2.amount > $threshold2
    AND $startTime < transfer1.timestamp AND transfer1.timestamp < $endTime 
    AND transfer1.amount > $threshold1
WITH 
    mid.id AS midId, 
    sum(transfer2.amount) as sumEdge2Amount,
    sum(transfer1.amount) AS sumEdge1Amount, 
    count(transfer1.amount) AS numEdge1
WHERE numEdge1 > 3
RETURN midId, round(sumEdge1Amount, 3), round(sumEdge2Amount, 3)
ORDER BY sumEdge2Amount DESC, midId
