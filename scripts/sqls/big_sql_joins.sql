SELECT 
name,
product_name as product_bought,
order_date
FROM orders 
JOIN customers ON orders.customer_id = customers.customer_id 
JOIN order_items ON orders.order_id = order_items.order_id
JOIN products ON order_items.product_id = products.product_id 
WHERE order_date = :target_order_date

