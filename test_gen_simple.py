from mock_generator import MockDataGenerator

def test_gen():
    gen = MockDataGenerator()
    mapping = {
        "ID": "increment",
        "NAME": "name",
        "EMAIL": "email",
        "CAT": "random_element(['A', 'B', 'C'])",
        "VAL": "random_int(min=10, max=20)",
        "PRICE": "random_number(digits=2)"
    }
    
    for i in range(3):
        print(gen.generate_record(mapping))

if __name__ == "__main__":
    test_gen()
