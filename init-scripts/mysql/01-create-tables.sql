-- Create payments table
CREATE TABLE IF NOT EXISTS trn_payments (
    payment_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    paid_at TIMESTAMP(3) NOT NULL,
    payment_method VARCHAR(50) DEFAULT 'credit_card',
    INDEX idx_order_id (order_id),
    INDEX idx_paid_at (paid_at)
);

-- Insert sample payments
INSERT IGNORE INTO trn_payments (payment_id, order_id, amount, paid_at, payment_method) VALUES
(2001, 1001, 150.00, '2024-01-01 10:05:00.000', 'credit_card'),
(2002, 1002, 75.50, '2024-01-01 11:35:00.000', 'debit_card'),
(2003, 1003, 200.00, '2024-01-02 09:20:00.000', 'credit_card'),
(2004, 1004, 125.75, '2024-01-02 14:25:00.000', 'paypal'),
(2005, 1005, 300.00, '2024-01-03 16:50:00.000', 'credit_card'),
(2006, 1006, 89.99, '2024-01-03 12:15:00.000', 'debit_card'),
(2007, 1007, 175.25, '2024-01-04 08:35:00.000', 'credit_card'),
-- Note: order 1008 has no payment (testing missing data scenario)
(2009, 1009, 220.50, '2024-01-05 13:50:00.000', 'credit_card'),
(2010, 1010, 165.00, '2024-01-05 17:25:00.000', 'paypal');