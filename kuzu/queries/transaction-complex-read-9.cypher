MATCH
  (loan1:Loan)-[edge1:LoanDepositAccount]->(mid:Account {id: $id})-[edge2:AccountRepayLoan]->(loan2:Loan),
  (up:Account)-[edge3:AccountTransferAccount]->(mid)-[edge4:AccountTransferAccount]->(down:Account)
WHERE edge1.amount > $threshold AND $startTime < edge1.timestamp AND edge1.timestamp < $endTime
  AND edge2.amount > $threshold AND $startTime < edge2.timestamp AND edge2.timestamp < $endTime
  AND edge3.amount > $threshold AND $startTime < edge3.timestamp AND edge3.timestamp < $endTime
  AND edge4.amount > $threshold AND $startTime < edge4.timestamp AND edge4.timestamp < $endTime
WITH
  sum(edge1.amount) AS edge1Amount, sum(edge2.amount) AS edge2Amount,
  sum(edge3.amount) AS edge3Amount, sum(edge4.amount) AS edge4Amount,
  count(edge2) AS edge2Count,
  count(edge4) AS edge4Count
WITH
  CASE edge2Count
    WHEN 0 THEN -1.0
    ELSE (1.0 * edge1Amount) / edge2Amount
  END AS ratioRepay,
  CASE edge4Count
    WHEN 0 THEN -1.0
    ELSE (1.0 * edge1Amount) / edge4Amount
  END AS ratioDeposit,
  CASE edge4Count
    WHEN 0 THEN -1.0
    ELSE (1.0 * edge3Amount) / edge4Amount
  END AS ratioTransfer
RETURN
  CAST(round(ratioRepay, 3), "FLOAT"),
  CAST(round(ratioDeposit, 3), "FLOAT"),
  CAST(round(ratioTransfer, 3), "FLOAT")
