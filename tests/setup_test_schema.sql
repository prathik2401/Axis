-- Axis Test Database Schema Setup
-- Run this on the SOURCE database (192.168.29.5:5432/source_db)

-- Drop tables if they exist (for clean testing)
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Create users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    phone VARCHAR(20),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create products table
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    stock INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create orders table
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert initial test data
INSERT INTO users (email, name, phone) VALUES
    ('john@example.com', 'John Doe', '555-1234'),
    ('jane@example.com', 'Jane Smith', '555-5678'),
    ('bob@example.com', 'Bob Johnson', '555-9012');

INSERT INTO products (name, description, price, stock) VALUES
    ('Laptop', 'High-performance laptop', 1299.99, 50),
    ('Mouse', 'Wireless mouse', 29.99, 200),
    ('Keyboard', 'Mechanical keyboard', 149.99, 100);

INSERT INTO orders (user_id, product_name, quantity, price) VALUES
    (1, 'Laptop', 1, 1299.99),
    (2, 'Mouse', 2, 29.99),
    (3, 'Keyboard', 1, 149.99);

-- Verify data
SELECT 'Users:' as table_name, COUNT(*) as row_count FROM users
UNION ALL
SELECT 'Products:', COUNT(*) FROM products
UNION ALL
SELECT 'Orders:', COUNT(*) FROM orders;

-- Show sample data
SELECT * FROM users;
SELECT * FROM products;
SELECT * FROM orders;
