
from faker import Faker
import random
import re
import datetime

class MockDataGenerator:
    def __init__(self):
        self.faker = Faker()
        self.increments = {}

    def generate_value(self, pattern, field_name=None):
        if pattern == "increment":
            val = self.increments.get(field_name, 0) + 1
            self.increments[field_name] = val
            return val
        
        # Check if it's a direct faker method call
        try:
            if hasattr(self.faker, pattern):
                val = getattr(self.faker, pattern)()
                # Convert date/datetime objects to ISO format string
                if hasattr(val, 'isoformat'):
                    if isinstance(val, datetime.datetime):
                        return val.isoformat(sep=' ', timespec='seconds')
                    return val.isoformat() # Returns YYYY-MM-DD for date objects
                # Convert decimal to float
                if hasattr(val, 'to_eng_string'): # Likely a decimal.Decimal
                    return float(val)
                return val
        except Exception:
            pass # Fallback to regex or guessing
        
        # Handle complex patterns like random_element(['A', 'B']) or random_int(min=1, max=100)
        match_element = re.match(r"random_element\\(\\[(.*)\\]\\)", pattern)
        if match_element:
            elements = [e.strip().strip("'").strip('"') for e in match_element.group(1).split(',')]
            return random.choice(elements)
        
        match_int = re.match(r"random_int\\(min=(\\d+), max=(\\d+)\\)", pattern)
        if match_int:
            return random.randint(int(match_int.group(1)), int(match_int.group(2)))

        match_num = re.match(r"random_number\\(digits=(\\d+)\\)", pattern)
        if match_num:
            return self.faker.random_number(digits=int(match_num.group(1)))

        # Default fallback: try to guess based on field name if pattern is not recognized
        if field_name:
            fn_lower = field_name.lower()
            if "name" in fn_lower:
                return self.faker.name()
            if "email" in fn_lower:
                return self.faker.email()
            if "postal" in fn_lower or "zip" in fn_lower:
                return self.faker.postcode()
            if "date" in fn_lower:
                return self.faker.date_this_decade().isoformat()
            if any(k in fn_lower for k in ["time", "created", "updated"]):
                return self.faker.date_time_this_decade().isoformat(sep=' ', timespec='seconds')
            if any(k in fn_lower for k in ["price", "amt", "amount", "salary", "balance"]):
                return round(random.uniform(10.0, 1000.0), 2)
            if any(k in fn_lower for k in ["qty", "stock", "count", "quantity"]):
                return random.randint(1, 100)
            if "id" in fn_lower:
                return random.randint(1, 1000)
        
        return self.faker.word()

    def generate_record(self, field_mapping):
        record = {}
        for field, pattern in field_mapping.items():
            record[field] = self.generate_value(pattern, field)
        return record

    def reset_increments(self):
        self.increments = {}
