INSERT INTO 
	public.kkar_dwh_dim_accounts( 
		account_num,
		valid_to,
		client,
		create_dt,
		update_dt 
	)
SELECT 
	stg.account_num,
	stg.valid_to,
	stg.client,
	stg.create_dt,
	NULL 
FROM public.kkar_stg_accounts stg
LEFT JOIN 
	public.kkar_dwh_dim_accounts tgt
ON 
	stg.account_num = tgt.account_num
WHERE
	tgt.account_num IS NULL;