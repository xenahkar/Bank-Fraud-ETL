--  type 1. Совершение операции при просроченном или заблокированном паспорте
INSERT INTO 
    public.kkar_rep_fraud(
        event_dt, 
        passport_num,
        fio, 
        phone,
        event_type,
        report_dt
    )
SELECT 
    kdft.trans_date AS event_dt,
    kddc2.passport_num AS passport_num,
    CONCAT(kddc2.last_name, ' ', kddc2.first_name, ' ', kddc2.patronymic) AS fio,
    kddc2.phone AS phone,
    1 AS event_type,
    TO_DATE(%s, 'YYYY-MM-DD') AS report_dt
FROM 
    public.kkar_dwh_fact_transactions kdft
LEFT JOIN 
    public.kkar_dwh_dim_cards kddc ON TRIM(kdft.card_num) = TRIM(kddc.card_num)
LEFT JOIN 
    public.kkar_dwh_dim_accounts kdda ON kddc.account_num = kdda.account_num
LEFT JOIN 
    public.kkar_dwh_dim_clients kddc2 ON kdda.client = kddc2.client_id
WHERE 
    (
    	-- заблокированный паспорт
    	kddc2.passport_num IN (SELECT passport_num FROM public.kkar_dwh_fact_passport_blacklist)
    	-- просроченный паспорт
	    OR kddc2.passport_valid_to < DATE(kdft.trans_date)
    );

-- type 2. Совершение операции при недействующем договоре
INSERT into public.kkar_rep_fraud(
	event_dt,
	passport_num,
	fio,
	phone,
	event_type,
	report_dt
	)
    SELECT
        kdft.trans_date AS event_dt,
	    kddc2.passport_num AS passport,
	    CONCAT(kddc2.last_name, ' ', kddc2.first_name, ' ', kddc2.patronymic) AS fio,
	    kddc2.phone AS phone,
        2 as event_type,
        TO_DATE(%s, 'YYYY-MM-DD') as report_dt
    FROM
	    public.kkar_dwh_fact_transactions kdft
	LEFT JOIN
	    public.kkar_dwh_dim_cards kddc ON TRIM(kdft.card_num) = TRIM(kddc.card_num)
	LEFT JOIN
	    public.kkar_dwh_dim_accounts kdda ON kddc.account_num = kdda.account_num
	LEFT JOIN
	    public.kkar_dwh_dim_clients kddc2 ON kdda.client = kddc2.client_id
	WHERE
	    kdda.valid_to < DATE(kdft.trans_date);

-- type 3. Совершение операций в разных городах в течение одного часа.
WITH ranked_transactions AS (
    SELECT
        event_dt,
        card_num,
        terminal_city,
        passport_num,
        fio,
        phone,
        ROW_NUMBER() OVER (PARTITION BY card_num ORDER BY event_dt) AS rn
    FROM
        (SELECT kdft.trans_date AS event_dt,
        		TRIM(kgdc.card_num) AS card_num,
        		kddt.terminal_city AS terminal_city,
     			kddc2.passport_num AS passport_num,
     			CONCAT(kddc2.last_name, ' ', kddc2.first_name, ' ', kddc2.patronymic) AS fio,
     			kddc2.phone AS phone
        		from public.kkar_dwh_fact_transactions kdft
			left join public.kkar_dwh_dim_cards kgdc ON TRIM(kdft.card_num) = TRIM(kgdc.card_num)
			left join public.kkar_dwh_dim_accounts kdda ON kgdc.account_num = kdda.account_num
			left join public.kkar_dwh_dim_clients kddc2 ON kdda.client = kddc2.client_id
			left join public.kkar_dwh_dim_terminals kddt ON kdft.terminal = kddt.terminal_id)
)
INSERT INTO
	public.kkar_rep_fraud(
		event_dt,
		passport_num,
		fio,
		phone,
		event_type,
		report_dt
	)
SELECT DISTINCT
    t1.event_dt,
    t1.passport_num,
    t1.fio,
    t1.phone,
	3 as event_type,
	to_date(%s, 'YYYY-MM-DD') as report_dt
FROM
    ranked_transactions t1
JOIN
    ranked_transactions t2 ON t1.card_num = t2.card_num
WHERE
    t1.terminal_city <> t2.terminal_city
    AND t1.rn <> t2.rn
    AND ABS(EXTRACT(EPOCH FROM (t1.event_dt - t2.event_dt))) <= 3600;
