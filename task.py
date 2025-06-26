import pickle
import pandas as pd 
from datetime import datetime

data_path = 'C:/Users/veron/OneDrive/Рабочий стол/ServiceTitan/Internship_Task/ServiceTitan_Internship/customer_orders.pkl'
vip_customers_path = 'C:/Users/veron/OneDrive/Рабочий стол/ServiceTitan/Internship_Task/ServiceTitan_Internship/vip_customers.txt'

class CustomerDataExtractor:
    def __init__(self, data_path, vip_customers_path):
        self.data_path = data_path
        self.vip_customers_path = vip_customers_path
        self.vip_ids = self.load_vip_ids()

    def load_vip_ids(self):
        with open(self.vip_customers_path, 'r') as file:
            return set(line.strip() for line in file if line.strip())

    def load_orders(self):
        with open(self.data_path, 'rb') as file:
            return pickle.load(file)

    def transform_data(self, customers):
        rows = []
        categories = {1: 'Electronics', 2: 'Apparel', 3: 'Books', 4: 'Home Goods'}

        for customer in customers:
            customer_id = customer['id']
            customer_name = customer['name']
            registration_date = datetime.strptime(customer['registration_date'], '%Y-%m-%d %H:%M:%S')
            is_vip = str(customer_id) in self.vip_ids

            for order in customer.get('orders', []):
                order_id = order.get('order_id', 0)
                order_date = datetime.strptime(order['order_date'], '%Y-%m-%d %H:%M:%S')
                order_total_value = order.get('order_total_value', 0)

                items = order.get('items', [])
                total_order_value = 0.0
                for item in items:
                    price = item.get('price', 0)
                    quantity = item.get('quantity', 1)
                    if price == 'FREE':
                        price_value = 0.0
                    elif isinstance(price, str) and price.startswith('$'):
                        try:
                            price_value = float(price.lstrip('$'))
                        except ValueError:
                            print(f"Warning: Invalid price format '{price}' for item in order {order_id}")
                            price_value = 0.0
                    elif isinstance(price, str) and price == 'INVALID':
                        print(f"Warning: Invalid price 'INVALID' for item in order {order_id}")
                        price_value = 0.0
                    elif isinstance(price, (int, float)):
                        price_value = float(price)
                    else:
                        print(f"Warning: Invalid price format '{price}' for item in order {order_id}")
                        price_value = 0.0
                    total_order_value += price_value * quantity

                for item in items:
                    product_id = item.get('item_id', 0)
                    product_name = item.get('product_name', 'Unknown')
                    category = categories.get(item.get('category', 'Misc'), item.get('category', 'Misc'))
                    if isinstance(category, int):
                        category = categories.get(category, 'Misc')

                    price = item.get('price', 0)
                    if price == 'FREE':
                        price_value = 0.0
                    elif isinstance(price, str) and price.startswith('$'):
                        try:
                            price_value = float(price.lstrip('$'))
                        except ValueError:
                            print(f"Warning: Invalid price format '{price}' for item in order {order_id}")
                            price_value = 0.0
                    elif isinstance(price, str) and price == 'INVALID':
                        print(f"Warning: Invalid price 'INVALID' for item in order {order_id}")
                        price_value = 0.0
                    elif isinstance(price, (int, float)):
                        price_value = float(price)
                    else:
                        print(f"Warning: Invalid price format '{price}' for item in order {order_id}")
                        price_value = 0.0

                    quantity = item.get('quantity', 1)
                    total_item_price = price_value * quantity
                    total_order_value_percentage = (total_item_price / total_order_value * 100) if total_order_value > 0 else 0.0
                    
                    rows.append([customer_id, customer_name, registration_date, is_vip,
                                order_id, order_date, product_id, product_name,
                                category, price, quantity, total_item_price,
                                total_order_value_percentage])

        df = pd.DataFrame(rows, columns=['customer_id', 'customer_name', 'registration_date',
                                        'is_vip', 'order_id', 'order_date', 'product_id',
                                        'product_name', 'category', 'unit_price',
                                        'item_quantity', 'total_item_price',
                                        'total_order_value_percentage'])
        return df.astype({
            'customer_id': int,
            'customer_name': str,
            'registration_date': 'datetime64[ns]',
            'is_vip': bool,
            'order_id': int,
            'order_date': 'datetime64[ns]',
            'product_id': int,
            'product_name': str,
            'category': str,
            'unit_price': float,
            'item_quantity': int,
            'total_item_price': float,
            'total_order_value_percentage': float
        })

    def extract_data(self):
        customers = self.load_orders()
        df = self.transform_data(customers)
        return df.sort_values(['customer_id', 'order_id', 'product_id'])
    
if __name__ == "__main__":
    extractor = CustomerDataExtractor('customer_orders.pkl', 'vip_customers.txt')
    result_df = extractor.extract_data()
    print(result_df.head())
    