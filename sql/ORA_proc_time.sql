DECLARE
  t1 NUMBER;
  t2 NUMBER;
  cnt NUMBER;
BEGIN
  t1 := DBMS_UTILITY.GET_TIME;
  SELECT COUNT(*) INTO cnt FROM (
.
.
.
) cd
	ON cd.contract_id = c.id 
	AND cd.rn = 1
WHERE c.conclusion_date 
	BETWEEN TO_DATE ('01.01.2025', 'dd.mm.yyyy')
    AND TO_DATE ('31.12.2025', 'dd.mm.yyyy')
ORDER BY np.short_name
  );
  t2 := DBMS_UTILITY.GET_TIME;
  DBMS_OUTPUT.PUT_LINE('-- время выполнения: ' || (t2 - t1) / 100 || ' сек');
  DBMS_OUTPUT.PUT_LINE('(контр 2025 стар чист 1802) количество строк: ' || cnt);
END;
