
CREATE TABLE IF NOT EXISTS public.phase9_people (
                                                    person_id BIGINT PRIMARY KEY,
                                                    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL UNIQUE,
    age INTEGER NOT NULL,
    city VARCHAR(100)
    );

CREATE TABLE IF NOT EXISTS public.phase9_orders (
                                                    order_id BIGINT PRIMARY KEY,
                                                    customer_name VARCHAR(100) NOT NULL,
    product_name VARCHAR(150) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL
    );
