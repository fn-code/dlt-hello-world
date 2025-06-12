import dlt

data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

pipeline = dlt.pipeline(
    pipeline_name="quick_start", destination="postgres", dataset_name="public"
)
load_info = pipeline.run(data, table_name="users")

print("===== Mantulity Response =====")
print(load_info)