def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 13
old_limit = 5
generator = square_root_generator(limit)
sum = 0

limit_count = 0
for sqrt_value in generator:
    print(sqrt_value)
    if limit_count < old_limit:
      sum += sqrt_value
      limit_count += 1

print(f"Sum is {sum}")
