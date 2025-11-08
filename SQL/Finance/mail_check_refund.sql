/* 
--------------------------------------------------------------------------------
SQL Script: Mail Refund Check
--------------------------------------------------------------------------------
Objective:
    Identify in-store orders that have been reversed and refunded by Check,
    to support reconciliation of refunds issued vs. accounting records.

Definition:
    - Mail Refund Check:
        • Original in-store sale followed by a reversing transaction
        • Refund tender type or refund method explicitly marked as CHECK
    - Reversed Order:
        • Transaction with a negative merchandise/total amount
        • Linked to an original sale via order/receipt reference

Scope:
    - Includes only in-store transactions.
    - Excludes ecommerce-only orders and non-monetary adjustments.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH parameterized_dates AS (
    SELECT
        @TransactionDateFrom AS transaction_date_from,
        -- End of day for @TransactionDateTo (23:59:59)
        DATEADD(SECOND, -1, DATEADD(DAY, 1, @TransactionDateTo)) AS transaction_date_to
),
base_transactions AS (
    SELECT
        a.organization_id,
        a.rtl_loc_id,
        a.business_date,
        a.wkstn_id,
        a.trans_seq,
        a.create_date,
        a.trans_statcode,
        a.post_void_flag,
        a.create_user_id,
        b.cust_party_id,
        b.create_user_id AS rtrans_create_user_id
    FROM trn_trans AS a
    INNER JOIN trl_rtrans AS b
        ON  a.organization_id = b.organization_id
        AND a.rtl_loc_id      = b.rtl_loc_id
        AND a.business_date   = b.business_date
        AND a.wkstn_id        = b.wkstn_id
        AND a.trans_seq       = b.trans_seq
    CROSS JOIN parameterized_dates AS p
    WHERE a.trans_statcode = 'COMPLETE'
      AND a.post_void_flag = 0
      AND a.create_date BETWEEN p.transaction_date_from AND p.transaction_date_to
      AND a.create_user_id <> 'DATALOADER'
      AND b.create_user_id <> 'DATALOADER'
),
check_tenders AS (
    SELECT
        bt.organization_id,
        bt.rtl_loc_id,
        bt.business_date,
        bt.wkstn_id,
        bt.trans_seq,
        bt.create_date,
        bt.cust_party_id,
        c.payable_to_name,
        c.payable_to_address,
        COALESCE(c.payable_to_address2, c.payable_to_apt) AS payable_to_apartment,
        c.payable_to_city,
        c.payable_to_state,
        c.payable_to_postal_code,
        c.payable_to_country,
        c.create_user_id AS send_check_create_user_id
    FROM base_transactions AS bt
    INNER JOIN ttr_send_check_tndr_lineitm AS c
        ON  bt.organization_id = c.organization_id
        AND bt.rtl_loc_id      = c.rtl_loc_id
        AND bt.business_date   = c.business_date
        AND bt.wkstn_id        = c.wkstn_id
        AND bt.trans_seq       = c.trans_seq
    WHERE c.create_user_id <> 'DATALOADER'
),
tender_lines AS (
    SELECT
        ct.organization_id,
        ct.rtl_loc_id,
        ct.business_date,
        ct.wkstn_id,
        ct.trans_seq,
        ct.create_date,
        ct.cust_party_id,
        ct.payable_to_name,
        ct.payable_to_address,
        ct.payable_to_apartment,
        ct.payable_to_city,
        ct.payable_to_state,
        ct.payable_to_postal_code,
        ct.payable_to_country,
        d.amt,
        d.tndr_id,
        d.create_user_id       AS tender_create_user_id,
        e.rtrans_lineitm_seq,
        e.void_flag,
        e.create_user_id       AS lineitm_create_user_id
    FROM check_tenders AS ct
    INNER JOIN ttr_tndr_lineitm AS d
        ON  ct.organization_id = d.organization_id
        AND ct.rtl_loc_id      = d.rtl_loc_id
        AND ct.business_date   = d.business_date
        AND ct.wkstn_id        = d.wkstn_id
        AND ct.trans_seq       = d.trans_seq
    INNER JOIN trl_rtrans_lineitm AS e
        ON  ct.organization_id = e.organization_id
        AND ct.rtl_loc_id      = e.rtl_loc_id
        AND ct.business_date   = e.business_date
        AND ct.wkstn_id        = e.wkstn_id
        AND ct.trans_seq       = e.trans_seq
        AND d.rtrans_lineitm_seq = e.rtrans_lineitm_seq
    WHERE d.tndr_id = 'HOME_OFFICE_CHECK'
      AND e.void_flag = 0
      AND d.create_user_id <> 'DATALOADER'
      AND e.create_user_id <> 'DATALOADER'
),
preferred_phone AS (
    SELECT
        t.party_id,
        t.telephone_number,
        ROW_NUMBER() OVER (
            PARTITION BY t.party_id
            ORDER BY 
                t.contact_flag DESC,        -- prefer flagged contacts
                t.create_date  DESC         -- most recently created
        ) AS phone_rank
    FROM crm_party_telephone AS t
    WHERE t.telephone_number IS NOT NULL
)

SELECT
    tl.rtl_loc_id   AS StoreId,
    tl.create_date  AS TransactionTimeStamp,
    ABS(tl.amt)     AS Amount,
    tl.tndr_id      AS TenderCode,
    tl.trans_seq    AS TransactionId,
    tl.wkstn_id     AS RegisterId,
    bt.cust_party_id          AS CustomerId,
    tl.payable_to_name        AS Name,
    tl.payable_to_address     AS Address,
    tl.payable_to_apartment   AS Apartment,
    tl.payable_to_city        AS City,
    tl.payable_to_state       AS State,
    tl.payable_to_postal_code AS ZipCode,
    tl.payable_to_country     AS Country,
    pp.telephone_number       AS Phone
FROM tender_lines AS tl
INNER JOIN base_transactions AS bt
    ON  tl.organization_id = bt.organization_id
    AND tl.rtl_loc_id      = bt.rtl_loc_id
    AND tl.business_date   = bt.business_date
    AND tl.wkstn_id        = bt.wkstn_id
    AND tl.trans_seq       = bt.trans_seq
LEFT JOIN preferred_phone AS pp
    ON pp.party_id = bt.cust_party_id
   AND pp.phone_rank = 1
ORDER BY
    StoreId,
    TransactionTimeStamp;
