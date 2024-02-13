def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}


import dlt

# define the connection to load to.
# We now use duckdb, but you can switch to Bigquery later
generators_pipeline = dlt.pipeline(destination='duckdb', dataset_name='generators')



# we can load any generator to a table at the pipeline destnation as follows:
info = generators_pipeline.run(people_1(),
								table_name="people",
                                primary_key="id",
								write_disposition="replace")


import duckdb

print(f"{generators_pipeline.pipeline_name}")

conn = duckdb.connect(f"{generators_pipeline.pipeline_name}.duckdb")

# let's see the tables
conn.sql(f"SET search_path = '{generators_pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))

# and the data

print("\n\n\n http_download table below:")

sum_age = conn.sql("SELECT sum(age) FROM people").df()
display(sum_age)


# the outcome metadata is returned by the load and we can inspect it by printing it.
print(info)

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


# we can load the next generator to the same or to a different table.
info = generators_pipeline.run(people_2(),
							   table_name="people",
                                primary_key="id",
								write_disposition="merge")



print(info)

sum_age = conn.sql("SELECT sum(age) FROM people").df()
display(sum_age)
