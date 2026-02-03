import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import json

fake = Faker()

# Product catalog
PRODUCTS = [
    {"id": "PROD_001", "name": "iPhone 15 Pro", "category": "Electronics", "brand": "Apple", "price": 999.99},
    {"id": "PROD_002", "name": "Samsung Galaxy S24", "category": "Electronics", "brand": "Samsung", "price": 899.99},
    {"id": "PROD_003", "name": "MacBook Air M3", "category": "Electronics", "brand": "Apple", "price": 1299.99},
    {"id": "PROD_004", "name": "Nike Air Max 270", "category": "Footwear", "brand": "Nike", "price": 150.00},
    {"id": "PROD_005", "name": "Adidas Ultraboost 22", "category": "Footwear", "brand": "Adidas", "price": 180.00},
    {"id": "PROD_006", "name": "Levi's 501 Jeans", "category": "Clothing", "brand": "Levi's", "price": 89.99},
    {"id": "PROD_007", "name": "Sony WH-1000XM5", "category": "Electronics", "brand": "Sony", "price": 399.99},
    {"id": "PROD_008", "name": "The North Face Jacket", "category": "Clothing", "brand": "The North Face", "price": 249.99},
    {"id": "PROD_009", "name": "Kindle Paperwhite", "category": "Electronics", "brand": "Amazon", "price": 139.99},
    {"id": "PROD_010", "name": "Yeti Rambler Tumbler", "category": "Home & Garden", "brand": "Yeti", "price": 39.99},
    {"id": "PROD_011", "name": "Instant Pot Duo 7-in-1", "category": "Home & Garden", "brand": "Instant Pot", "price": 99.99},
    {"id": "PROD_012", "name": "Fitbit Charge 6", "category": "Electronics", "brand": "Fitbit", "price": 199.99},
    {"id": "PROD_013", "name": "Patagonia Fleece Jacket", "category": "Clothing", "brand": "Patagonia", "price": 179.99},
    {"id": "PROD_014", "name": "Hydro Flask Water Bottle", "category": "Sports & Outdoors", "brand": "Hydro Flask", "price": 44.99},
    {"id": "PROD_015", "name": "Allbirds Tree Runners", "category": "Footwear", "brand": "Allbirds", "price": 98.00}
]

CATEGORIES = ["Electronics", "Clothing", "Footwear", "Home & Garden", "Sports & Outdoors", "Books", "Beauty", "Automotive"]
BRANDS = ["Apple", "Samsung", "Nike", "Adidas", "Amazon", "Google", "Microsoft", "Sony", "LG", "HP"]

# Geographic data
COUNTRIES = ["US", "CA", "UK", "DE", "FR", "AU", "JP", "BR", "IN", "MX"]
US_STATES = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
CITIES = {
    "US": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"],
    "CA": ["Toronto", "Montreal", "Vancouver", "Calgary", "Edmonton", "Ottawa", "Winnipeg", "Quebec City", "Hamilton", "Kitchener"],
    "UK": ["London", "Birmingham", "Manchester", "Glasgow", "Liverpool", "Leeds", "Sheffield", "Edinburgh", "Bristol", "Cardiff"],
    "DE": ["Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt", "Stuttgart", "Düsseldorf", "Dortmund", "Essen", "Leipzig"]
}

# Device and browser data
DEVICES = ["Desktop", "Mobile", "Tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Opera"]
OS_SYSTEMS = ["Windows", "macOS", "iOS", "Android", "Linux"]

# Traffic sources
TRAFFIC_SOURCES = ["organic", "paid_search", "social", "email", "direct", "referral", "affiliate"]
SOCIAL_PLATFORMS = ["facebook", "instagram", "twitter", "linkedin", "tiktok", "youtube", "pinterest"]

class EcommerceDataGenerator:
    def __init__(self):
        self.customers = self._generate_customers(1000)
        self.sessions = {}
        
    def _generate_customers(self, count):
        """Generate a pool of realistic customers"""
        customers = []
        for _ in range(count):
            country = random.choice(COUNTRIES)
            customer = {
                "customer_id": f"CUST_{str(uuid.uuid4())[:8].upper()}",
                "email": fake.email(),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "phone": fake.phone_number(),
                "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
                "gender": random.choice(["M", "F", "Other"]),
                "country": country,
                "state": random.choice(US_STATES) if country == "US" else None,
                "city": random.choice(CITIES.get(country, ["Unknown"])),
                "postal_code": fake.postcode(),
                "registration_date": fake.date_between(start_date='-2y', end_date='today').isoformat(),
                "customer_segment": random.choice(["Premium", "Standard", "Basic"]),
                "lifetime_value": round(random.uniform(50, 5000), 2),
                "is_active": random.choice([True, True, True, False])  # 75% active
            }
            customers.append(customer)
        return customers
    
    def get_random_customer(self):
        """Get a random customer from the pool"""
        return random.choice(self.customers)
    
    def generate_session_data(self, customer_id=None):
        """Generate realistic session data"""
        if not customer_id:
            customer = self.get_random_customer()
            customer_id = customer["customer_id"]
        
        session_id = str(uuid.uuid4())
        device = random.choice(DEVICES)
        
        session = {
            "session_id": session_id,
            "customer_id": customer_id,
            "start_time": datetime.now().isoformat(),
            "device_type": device,
            "browser": random.choice(BROWSERS),
            "operating_system": random.choice(OS_SYSTEMS),
            "screen_resolution": random.choice(["1920x1080", "1366x768", "1440x900", "1536x864", "1280x720"]),
            "user_agent": fake.user_agent(),
            "ip_address": fake.ipv4(),
            "traffic_source": random.choice(TRAFFIC_SOURCES),
            "utm_campaign": fake.word() if random.random() < 0.3 else None,
            "utm_medium": random.choice(["cpc", "email", "social", "organic"]) if random.random() < 0.3 else None,
            "utm_source": random.choice(SOCIAL_PLATFORMS) if random.random() < 0.3 else None,
            "referrer_url": fake.url() if random.random() < 0.4 else None,
            "landing_page": random.choice(["/", "/products", "/sale", "/new-arrivals", "/categories/electronics"]),
            "is_mobile": device == "Mobile",
            "is_new_visitor": random.choice([True, False]),
            "page_views": 0,
            "session_duration": 0,
            "bounce_rate": 0
        }
        
        self.sessions[session_id] = session
        return session
    
    def generate_page_view_event(self, session_id=None):
        """Generate realistic page view event"""
        if not session_id or session_id not in self.sessions:
            session = self.generate_session_data()
            session_id = session["session_id"]
        else:
            session = self.sessions[session_id]
        
        pages = [
            "/", "/products", "/categories/electronics", "/categories/clothing", 
            "/categories/footwear", "/search", "/cart", "/checkout", "/account",
            "/product/iphone-15-pro", "/product/macbook-air-m3", "/product/nike-air-max",
            "/sale", "/new-arrivals", "/about", "/contact", "/help", "/returns"
        ]
        
        page_url = random.choice(pages)
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "PAGE_VIEW",
            "session_id": session_id,
            "customer_id": session["customer_id"],
            "timestamp": datetime.now().isoformat(),
            "page_url": page_url,
            "page_title": self._get_page_title(page_url),
            "previous_page": session.get("last_page"),
            "time_on_page": random.randint(5, 300),  # seconds
            "scroll_depth": random.randint(10, 100),  # percentage
            "device_type": session["device_type"],
            "browser": session["browser"],
            "traffic_source": session["traffic_source"],
            "is_bounce": random.choice([True, False]) if session["page_views"] == 0 else False
        }
        
        # Update session
        session["page_views"] += 1
        session["last_page"] = page_url
        
        return event
    
    def generate_product_interaction_event(self, session_id=None):
        """Generate product interaction events (view, add to cart, etc.)"""
        if not session_id or session_id not in self.sessions:
            session = self.generate_session_data()
            session_id = session["session_id"]
        else:
            session = self.sessions[session_id]
        
        product = random.choice(PRODUCTS)
        interaction_type = random.choice(["PRODUCT_VIEW", "ADD_TO_CART", "REMOVE_FROM_CART", "ADD_TO_WISHLIST"])
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": interaction_type,
            "session_id": session_id,
            "customer_id": session["customer_id"],
            "timestamp": datetime.now().isoformat(),
            "product_id": product["id"],
            "product_name": product["name"],
            "product_category": product["category"],
            "product_brand": product["brand"],
            "product_price": product["price"],
            "quantity": random.randint(1, 5) if interaction_type in ["ADD_TO_CART", "REMOVE_FROM_CART"] else 1,
            "variant": random.choice(["Red", "Blue", "Black", "White", "Small", "Medium", "Large"]) if random.random() < 0.5 else None,
            "discount_applied": random.uniform(0, 0.3) if random.random() < 0.2 else 0,
            "device_type": session["device_type"],
            "page_url": f"/product/{product['id'].lower()}"
        }
        
        return event
    
    def generate_purchase_event(self, session_id=None):
        """Generate realistic purchase/transaction event"""
        if not session_id or session_id not in self.sessions:
            session = self.generate_session_data()
            session_id = session["session_id"]
        else:
            session = self.sessions[session_id]
        
        # Generate cart items
        num_items = random.randint(1, 5)
        cart_items = random.sample(PRODUCTS, num_items)
        
        subtotal = sum(item["price"] * random.randint(1, 3) for item in cart_items)
        tax_rate = 0.08
        shipping_cost = 0 if subtotal > 50 else 9.99
        discount = subtotal * random.uniform(0, 0.2) if random.random() < 0.3 else 0
        
        total_amount = subtotal + (subtotal * tax_rate) + shipping_cost - discount
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "PURCHASE",
            "session_id": session_id,
            "customer_id": session["customer_id"],
            "timestamp": datetime.now().isoformat(),
            "order_id": f"ORD_{str(uuid.uuid4())[:8].upper()}",
            "items": [
                {
                    "product_id": item["id"],
                    "product_name": item["name"],
                    "category": item["category"],
                    "brand": item["brand"],
                    "price": item["price"],
                    "quantity": random.randint(1, 3)
                } for item in cart_items
            ],
            "subtotal": round(subtotal, 2),
            "tax_amount": round(subtotal * tax_rate, 2),
            "shipping_cost": shipping_cost,
            "discount_amount": round(discount, 2),
            "total_amount": round(total_amount, 2),
            "currency": "USD",
            "payment_method": random.choice(["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]),
            "shipping_method": random.choice(["standard", "express", "overnight", "pickup"]),
            "coupon_code": fake.word().upper() if random.random() < 0.2 else None,
            "device_type": session["device_type"],
            "is_guest_checkout": random.choice([True, False])
        }
        
        return event
    
    def generate_search_event(self, session_id=None):
        """Generate search event"""
        if not session_id or session_id not in self.sessions:
            session = self.generate_session_data()
            session_id = session["session_id"]
        else:
            session = self.sessions[session_id]
        
        search_terms = [
            "iphone", "macbook", "nike shoes", "jeans", "headphones", "jacket",
            "water bottle", "fitness tracker", "laptop", "smartphone", "sneakers",
            "winter coat", "bluetooth speaker", "tablet", "running shoes"
        ]
        
        search_query = random.choice(search_terms)
        results_count = random.randint(0, 500)
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "SEARCH",
            "session_id": session_id,
            "customer_id": session["customer_id"],
            "timestamp": datetime.now().isoformat(),
            "search_query": search_query,
            "search_results_count": results_count,
            "search_category": random.choice(CATEGORIES) if random.random() < 0.3 else None,
            "search_filters": {
                "price_min": random.randint(0, 100) if random.random() < 0.2 else None,
                "price_max": random.randint(200, 1000) if random.random() < 0.2 else None,
                "brand": random.choice(BRANDS) if random.random() < 0.3 else None,
                "rating": random.choice([3, 4, 5]) if random.random() < 0.2 else None
            },
            "clicked_result_position": random.randint(1, min(10, results_count)) if results_count > 0 and random.random() < 0.6 else None,
            "device_type": session["device_type"]
        }
        
        return event
    
    def generate_user_engagement_event(self, session_id=None):
        """Generate user engagement events (reviews, ratings, etc.)"""
        if not session_id or session_id not in self.sessions:
            session = self.generate_session_data()
            session_id = session["session_id"]
        else:
            session = self.sessions[session_id]
        
        engagement_types = ["REVIEW", "RATING", "SHARE", "NEWSLETTER_SIGNUP", "ACCOUNT_UPDATE"]
        engagement_type = random.choice(engagement_types)
        
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": engagement_type,
            "session_id": session_id,
            "customer_id": session["customer_id"],
            "timestamp": datetime.now().isoformat(),
            "device_type": session["device_type"]
        }
        
        if engagement_type == "REVIEW":
            product = random.choice(PRODUCTS)
            event.update({
                "product_id": product["id"],
                "rating": random.randint(1, 5),
                "review_text": fake.text(max_nb_chars=200),
                "is_verified_purchase": random.choice([True, False])
            })
        elif engagement_type == "RATING":
            product = random.choice(PRODUCTS)
            event.update({
                "product_id": product["id"],
                "rating": random.randint(1, 5)
            })
        elif engagement_type == "SHARE":
            event.update({
                "shared_content": random.choice(["product", "category", "sale"]),
                "share_platform": random.choice(SOCIAL_PLATFORMS),
                "shared_url": fake.url()
            })
        elif engagement_type == "NEWSLETTER_SIGNUP":
            event.update({
                "email": fake.email(),
                "subscription_type": random.choice(["weekly", "monthly", "promotional"])
            })
        
        return event
    
    def _get_page_title(self, page_url):
        """Get realistic page title based on URL"""
        titles = {
            "/": "Home - E-commerce Store",
            "/products": "All Products - E-commerce Store",
            "/categories/electronics": "Electronics - E-commerce Store",
            "/categories/clothing": "Clothing - E-commerce Store",
            "/categories/footwear": "Footwear - E-commerce Store",
            "/search": "Search Results - E-commerce Store",
            "/cart": "Shopping Cart - E-commerce Store",
            "/checkout": "Checkout - E-commerce Store",
            "/account": "My Account - E-commerce Store",
            "/sale": "Sale Items - E-commerce Store",
            "/new-arrivals": "New Arrivals - E-commerce Store"
        }
        return titles.get(page_url, f"{page_url.replace('/', '').title()} - E-commerce Store")
    
    def generate_random_event(self, session_id=None):
        """Generate a random event type"""
        event_generators = [
            (self.generate_page_view_event, 0.4),
            (self.generate_product_interaction_event, 0.3),
            (self.generate_search_event, 0.15),
            (self.generate_purchase_event, 0.1),
            (self.generate_user_engagement_event, 0.05)
        ]
        
        # Weighted random selection
        rand = random.random()
        cumulative = 0
        
        for generator, weight in event_generators:
            cumulative += weight
            if rand <= cumulative:
                return generator(session_id)
        
        # Fallback
        return self.generate_page_view_event(session_id)
    
    def add_data_quality_issues(self, event, error_rate=0.15):
        """Add realistic data quality issues"""
        if random.random() > error_rate:
            return event, True  # No issues
        
        # Introduce various data quality issues
        issues = []
        
        # Missing required fields
        if random.random() < 0.3:
            required_fields = ["customer_id", "session_id", "timestamp"]
            field_to_remove = random.choice(required_fields)
            if field_to_remove in event:
                event[field_to_remove] = None
                issues.append(f"missing_{field_to_remove}")
        
        # Invalid data types
        if random.random() < 0.2 and "total_amount" in event:
            event["total_amount"] = "invalid_amount"
            issues.append("invalid_amount_format")
        
        # Future timestamps
        if random.random() < 0.1:
            future_time = datetime.now() + timedelta(days=random.randint(1, 30))
            event["timestamp"] = future_time.isoformat()
            issues.append("future_timestamp")
        
        # Negative values where they shouldn't be
        if random.random() < 0.15 and "product_price" in event:
            event["product_price"] = -abs(event["product_price"])
            issues.append("negative_price")
        
        # Add metadata about issues
        event["data_quality_issues"] = issues
        event["is_valid"] = len(issues) == 0
        
        return event, len(issues) == 0