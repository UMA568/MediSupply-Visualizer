WITH cte AS (
    SELECT orderautoid,
        store_tables.CITY,
        add_ress.pincode,
        CASE 
            WHEN store_tables.CITY IN ('Delhi-NCR', 'Mumbai', 'Chennai', 'Hyderabad', 'Kolkata', 'Bangalore') THEN 'Top6_city'
            WHEN store_tables.CITY IN ('Visakhapatnam', 'Chandigarh', 'Vijayawada', 'Lucknow', 'Pune', 'Bhubaneswar', 'Puri', 'Cuttack', 'Ahmedabad', 'Jaipur') 
            THEN 'next10_city'
            ELSE 'others' 
        END AS store_city_bucket,
        order_table.shopid,
        CASE 
            WHEN instant_store = 'Yes' THEN 'instant' 
            ELSE Store_type 
        END AS store_type,
        LATESTORDERSTATUS
    FROM order_table
    LEFT JOIN add_ress ON order_table.orderautoid = add_ress.medicine_orders_order_auto_id
    LEFT JOIN store_tables ON store_tables.codess = order_table.shopid
), 
cte2 AS (
    SELECT cte.*, systemdsp, dsp,
        CASE 
            WHEN LATESTORDERSTATUS = 'DELIVERED' THEN 'Delivered'
            WHEN LATESTORDERSTATUS = 'CANCELLED' THEN 'Cancelled'
            WHEN LATESTORDERSTATUS IN ('ORDER_VERIFIED', 'RIDER_REACHED_STORE', 'READY_TO_SHIP', 'PACKED', 'RIDER_REACHED_CUSTOMER_GATE', 'CONSULT_PENDING', 'ON_HOLD',
            'ORDER_BILLED', 'DELIVERY_ATTEMPTED') THEN 'Pending'
            WHEN LATESTORDERSTATUS IN ('RETURN_REFUND_INITIATED', 'RETURN_REQUEST_RTO', 'RETURN_OMS_CANCEL_REQUEST_SUCCESS', 'RETURN_TO_ORIGIN', 
            'RETURN_REQUEST_REJECTED', 'RETURN_REQUEST_ON_HOLD', 'RETURN_PENDING', 'RETURN_MANUAL_VERIFICATION', 'RETURN_REQUEST_RVP_ASSIGNED',
            'RETURN_REQUEST_CANCELLED', 'RETURN_REQUEST_OMS_DENIED') THEN 'Return' 
        END AS pendency_flag, 
        
date(date_format(firsttat, '%Y-%m-%d %H:%i:%s'))AS firststat__date,
date_format(firsttat, '%Y-%m-%d %H:%i:%s')AS firststat_datetime,
date(date_format(secondtat, '%Y-%m-%d %H:%i:%s'))AS secondstat_Date, 
date_format(secondtat, '%Y-%m-%d %H:%i:%s') AS secondstat_Datetime,
date(date_format(latesttat, '%Y-%m-%d %H:%i:%s')) AS lateststat_Date, 
date_format(latesttat, '%Y-%m-%d %H:%i:%s') AS lateststat_Datetime
    FROM cte
    LEFT JOIN shipments s ON s.orderid = cte.orderautoid
    LEFT JOIN dsp_ta ON TRIM(s.systemdsp) = TRIM(dsp_ta.providers)
    LEFT JOIN tat_table ON cte.orderautoid = tat_table.orderids
), 
cte3 AS (
    SELECT MEDICINEORDERSORDERAUTOID,
        MAX(CASE 
            WHEN orderstatuss = 'DELIVERED' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) AS Packed_date,
        MAX(CASE 
            WHEN orderstatuss IN ('ORDER_INITIATED', 'PAYMENT_SUCCESS', 'CONSULT_COMPLETED', 'PRESCRIPTION_UPLOADED') 
            THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) AS Booked_date,
        MAX(CASE
            WHEN orderstatuss = 'ON_HOLD' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) AS Onhold,
        MAX(CASE
        WHEN orderstatuss IN ('PRESCRIPTION_VERIFIED', 'VERIFICATION_DONE') THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) AS Verified,
        MAX(CASE
        WHEN Orderstatuss='ORDER_VERIFIED' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) AS shc,
        MAX(CASE
        WHEN orderstatuss= 'ORDER_BILLED' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) AS Billed_Date,
        MAX(CASE
        WHEN orderstatuss = 'RIDER_REACHED_STORE' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) AS Rider_Reached_Store,
        MAX(CASE
        WHEN orderstatuss ='PACKED' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) AS Packed,
        MAX(CASE
        WHEN orderstatuss= 'READY_TO_SHIP' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) AS Ready_To_Ship,
        MAX(CASE
        WHEN orderstatuss ='SHIPPED' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) AS Shipped_Date,
        MAX(CASE
        WHEN orderstatuss='OUT_FOR_DELIVERY' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) AS Out_For_Delivery,
        MAX(CASE
        WHEN orderstatuss='DELIVERY_ATTEMPTED' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) Delivery_attempted,  
        MAX(CASE 
        WHEN orderstatuss='RIDER_REACHED_CUSTOMER_GATE' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) Rider_Reached_Cust_Gate,  
        MAX(CASE
        WHEN orderstatuss='DELIVERED' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) Delivered,
        MAX(CASE
        WHEN orderstatuss='RETURN_PENDING' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) Return_Pending,
        MAX(CASE
        WHEN orderstatuss='RETURN_REQUESTED' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) Return_Requested,
        MAX(CASE
        WHEN orderstatuss='CANCELLED' THEN DATE_ADD(STR_TO_DATE(statusdate, '%d-%m-%Y %H:%i:%s' ), INTERVAL 330 minute) 
        END) Cancelled
    FROM statuss 
    WHERE MEDICINEORDERSORDERAUTOID IN (SELECT orderautoid FROM orders_table)
    GROUP BY MEDICINEORDERSORDERAUTOID
)
SELECT * FROM cte2 
LEFT JOIN cte3 ON TRIM(cte2.orderautoid) = TRIM(cte3.MEDICINEORDERSORDERAUTOID) ;