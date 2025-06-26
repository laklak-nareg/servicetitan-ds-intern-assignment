import pickle
import pandas as pd

with open('customer_orders.pkl', 'rb') as file:
    data = pickle.load(file)

class DataExtractor:
    CATAGORY_MAP = {
        1: 'Electronics',
        2: 'Apparel',
        3: 'Books',
        4: 'Home Goods'
    }

    def __init__(self, customer_orders, vip_customers):
        self.customer_orders = customer_orders
        self.vip_customers = vip_customers
        self.vip_ids = self._load_vip_ids()
        self.data = self._load_customer_data()

    def _load_customer_data(self):
        with open(self.customer_orders, 'rb') as file:
            return pickle.load(file)

    def _load_vip_ids(self):
        with open(self.vip_customers) as file:
            return {int(line.strip()) for line in file if line.strip().isdigit()}

    def _clean_price(self, price):
        try:
            return float(str(price).replace('$', '').strip())
        except:
            return 0.0

    def _clean_quantity(self, quantity):
        try:
            return int(quantity)
        except:
            return 0

    def _clean_id(self, value):
        try:
            return int(value)
        except:
            return None

    def _is_vip(self, customer_id):
        return customer_id in self.vip_ids

    def _normalize_category(self, catagory):
        if isinstance(catagory, int):
            return self.CATAGORY_MAP.get(catagory, 'Misc')
        elif isinstance(catagory, str):
            clean_catagory = catagory.strip().lower()
            for value in self.CATAGORY_MAP.values():
                if value.lower() == clean_catagory:
                    return value
        return 'Misc'

    def extract_data(self):
        records = []
        for customer in self.data:
            customer_id = self._clean_id(customer.get('id'))
            customer_name = customer.get('name', '').strip()
            registration_date = pd.to_datetime(customer.get('registration_date'), errors='coerce')
            is_vip = self._is_vip(customer_id)

            orders = customer.get('orders', [])
            for order in orders:
                order_id = self._clean_id(order.get('order_id'))
                order_date = pd.to_datetime(order.get('order_date'), errors='coerce')
                order_total_value = order.get('order_total_value') or 0.0

                items = order.get('items', [])
                for item in items:
                    product_id = self._clean_id(item.get('item_id'))
                    product_name = item.get('product_name', '').strip()
                    category = self._normalize_category(item.get('category'))
                    unit_price = self._clean_price(item.get('price'))
                    item_quantity = self._clean_quantity(item.get('quantity'))
                    total_item_price = unit_price * item_quantity
                    
                    if order_total_value > 0:
                        total_order_value_percentage = (total_item_price / order_total_value)
                    else:
                        total_order_value_percentage = 0

                    if product_id is None or not product_id:
                        continue

                    if item_quantity > 0 and unit_price == 0.0:
                        continue

                    records.append({
                        'customer_id': customer_id,
                        'customer_name': customer_name,
                        'registration_date': registration_date,
                        'is_vip': is_vip,
                        'order_id': order_id,
                        'order_date': order_date,
                        'product_id': product_id,
                        'product_name': product_name,
                        'category': category,
                        'unit_price': unit_price,
                        'item_quantity': item_quantity,
                        'total_item_price': total_item_price,
                        'total_order_value_percentage': total_order_value_percentage
                    })

        df = pd.DataFrame(records)

        df = df.fillna({
            'order_id': 0,
            'product_id': 0,
            'registration_date': pd.NaT,
        })

        df = df.astype({
            'customer_id': 'int',
            'customer_name': 'str',
            'registration_date': 'datetime64[ns]',
            'is_vip': 'bool',
            'order_id': 'int',
            'order_date': 'datetime64[ns]',
            'product_id': 'int',
            'product_name': 'str',
            'category': 'str',
            'unit_price': 'float',
            'item_quantity': 'int',
            'total_item_price': 'float',
            'total_order_value_percentage': 'float'
        })

        df.sort_values(by=['customer_id', 'order_id', 'product_id'], inplace=True)
        return df

    def save_to_csv(self, df, filename='customer_data_cleaned_vip.csv'):
        df.to_csv(filename, index=False)

        invalid_item_price = df[(df['total_item_price'] == 0) & (df['item_quantity'] > 0)]
        print("Rows with zero total_item_price and non-zero item_quantity:")
        print(invalid_item_price)

        negative_percentage = df[df['total_order_value_percentage'] < 0]
        print("Rows with negative total_order_value_percentage:")
        print(negative_percentage)

        invalid_values = df[(df['unit_price'] <= 0) | (df['item_quantity'] <= 0)]
        print("Rows with zero or negative unit_price or item_quantity:")
        print(invalid_values)

        invalid_registration_date = df[df['registration_date'].isnull()]
        print("Rows with missing registration_date:")
        print(invalid_registration_date)

        df.dropna(subset=['order_id', 'product_id', 'product_name'], inplace=True)
        return df

if __name__ == "__main__":
    extractor = DataExtractor('customer_orders.pkl', 'vip_customers.txt')
    df = extractor.extract_data()
    extractor.save_to_csv(df)

    print("Extraction and export are complete, Preview: ")
    print(df.head())
