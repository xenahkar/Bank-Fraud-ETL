INSERT INTO 
    public.kkar_dwh_dim_cards(
        card_num,
        account_num,
        create_dt,
        update_dt 
    )
SELECT 
    stg.card_num,
    stg.account_num,
    stg.create_dt,
    NULL 
FROM public.kkar_stg_cards stg
LEFT JOIN 
    public.kkar_dwh_dim_cards tgt
ON 
    stg.card_num = tgt.card_num
WHERE
    tgt.card_num IS NULL;
