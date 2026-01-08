-- Create customers table
CREATE TABLE IF NOT EXISTS trn_customers (
    customer_id BIGINT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE IF NOT EXISTS trn_orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    order_ts TIMESTAMP NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES trn_customers(customer_id)
);

-- Insert sample customers
INSERT INTO trn_customers (customer_id, customer_name, email) VALUES
(1, 'John Doe', 'john.doe@example.com'),
(2, 'Jane Smith', 'jane.smith@example.com'),
(3, 'Bob Johnson', 'bob.johnson@example.com'),
(4, 'Alice Brown', 'alice.brown@example.com'),
(5, 'Charlie Wilson', 'charlie.wilson@example.com')
ON CONFLICT (customer_id) DO NOTHING;

-- Insert sample orders
INSERT INTO trn_orders (order_id, customer_id, order_ts, total_amount) VALUES
(1001, 1, '2024-01-01 10:00:00', 150.00),
(1002, 2, '2024-01-01 11:30:00', 75.50),
(1003, 1, '2024-01-02 09:15:00', 200.00),
(1004, 3, '2024-01-02 14:20:00', 125.75),
(1005, 4, '2024-01-03 16:45:00', 300.00),
(1006, 2, '2024-01-03 12:10:00', 89.99),
(1007, 5, '2024-01-04 08:30:00', 175.25),
(1008, 3, '2024-01-04 19:00:00', 95.00),
(1009, 1, '2024-01-05 13:45:00', 220.50),
(1010, 4, '2024-01-05 17:20:00', 165.00)
ON CONFLICT (order_id) DO NOTHING;