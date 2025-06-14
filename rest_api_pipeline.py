import dlt
import pandas as pd
import os
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources, EndpointResource, Endpoint
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from requests import Request

data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


# pipeline = dlt.pipeline(
#     pipeline_name="quick_start", destination="postgres", dataset_name="public"
# )
# load_info = pipeline.run(data, table_name="users", loader_file_format="csv")

# pipeline = dlt.pipeline(
#     pipeline_name="quick_start",
#     destination="filesystem",
#     dataset_name="products",
#     # full_refresh=True # Optional: If you want to overwrite previous runs
# )
# load_info = pipeline.run(
#     data,
#     table_name="users",
#     loader_file_format="csv"
# )

# # 1. Define your data source (e.g., a simple pandas DataFrame)
# def get_my_data():
#     """Generates some sample data."""
#     data = {
#         'product_id': [101, 102, 103, 104, 105],
#         'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Webcam'],
#         'price': [1200.50, 25.00, 75.99, 300.00, 49.99],
#         'in_stock': [True, True, False, True, True]
#     }
#     df = pd.DataFrame(data)
#     # dlt expects iterable data, so convert DataFrame to list of dictionaries
#     return df.to_dict(orient="records")
#
# # 2. Configure the dlt pipeline and destination
# # We'll explicitly set the destination to 'filesystem' and specify the output path.
# # 'file_format="csv"' is key here for CSV output.
# pipeline = dlt.pipeline(
#     pipeline_name="local_csv_example",
#     destination="filesystem",
#     dataset_name="products", # This will be the directory name for your output files
#     full_refresh=True # Optional: If you want to overwrite previous runs
#
# )
#
#
# # 3. Run the pipeline
# # The get_my_data function will be treated as a dlt resource.
# # The table_name argument defines the name of the CSV file (without extension)
# # within the dataset directory.
# load_info = pipeline.run(get_my_data(), table_name="product_details", loader_file_format="csv")
#

class PostBodyPaginator(OffsetPaginator):
    """
    Custom paginator that uses a POST request body to paginate through results.
    This is useful when the API requires pagination parameters to be sent in the request body.
    """
    def __init__(self, limit_param: str, offset_param: str, limit: int = 100, stop_after_empty_page: bool = True, total_path: str = None, maximum_offset: int = None, offset: int = 0,):
        super().__init__(
            limit_param=limit_param,
            offset_param=offset_param,
            limit=limit,
            stop_after_empty_page=stop_after_empty_page,
            total_path=total_path,
            maximum_offset=maximum_offset,
            offset=offset,
        )
        self.paginator_type = "post_body"  # Custom paginator type for identification

    def update_request(self, request: Request) -> None:
        """
        Update the request to include pagination parameters in the POST body.
        This method modifies the request to include the limit and offset parameters
        in the JSON body of a POST request.
        """
        if not request.json:
            request.json = {}

        request.json[self.limit_param] = self.limit
        request.json[self.param_name] = self.current_value

        # Ensure that the request method is POST
        request.method = "POST"

@dlt.source
def nf_list_matkul_source():
    config: RESTAPIConfig = {
        "client": {
            "base_url": "http://neofeeder.ung.ac.id:8100/",
        },
        "resource_defaults": {
            "write_disposition": "append",
        },
        "resources": [
            # Explicitly create an instance of EndpointResource here
            EndpointResource(
                name="matkul_list",
                endpoint=Endpoint(
                    path="",  # path url
                    method="POST",
                    json={
                        "act": "GetListMataKuliah",
                        "token": "",
                        "filter": "",
                        "order": "",
                        "limit": 500,
                        "offset": 0,
                    },
                    data_selector="data",  # This is a direct parameter to EndpointResource
                    # Use PostBodyPaginator to handle pagination via POST body
                    paginator=PostBodyPaginator(
                        limit_param="limit",
                        offset_param="offset",
                        limit=500,  # Maximum number of items per page
                        stop_after_empty_page=True,  # Stop pagination when an empty page is encountered
                        total_path=None,
                        maximum_offset=27000,  # Optional: Limit the maximum offset to prevent infinite loops
                    ),
                    # paginator=OffsetPaginator(
                    #     limit_param="limit",
                    #     offset_param="offset",
                    #     limit=5,  # Maximum number of items per page
                    #     stop_after_empty_page=True,  # Stop pagination when an empty page is encountered
                    #     total_path=None,
                    #     maximum_offset=10,  # Optional: Limit the maximum offset to prevent infinite loops
                    # )
                )
            ),
        ],
    }

    yield from rest_api_resources(config)


@dlt.source
def nf_list_prodi_source():
    config: RESTAPIConfig = {
        "client": {
            "base_url": "http://neofeeder.ung.ac.id:8100/",
        },
        "resource_defaults": {
            "write_disposition": "append",
        },
        "resources": [
            # Explicitly create an instance of EndpointResource here
            EndpointResource(
                name="prodi_list",
                endpoint=Endpoint(
                    path="", # path url
                    method="POST",
                    json={
                        "act": "GetProdi",
                        "token": "",
                        "filter": "",
                        "order": "",
                        "limit": 500,
                        "offset": 0,
                    },
                    data_selector="data",
                    paginator=PostBodyPaginator(
                        limit_param="limit",
                        offset_param="offset",
                        limit=500,  # Maximum number of items per page
                        stop_after_empty_page=True,  # Stop pagination when an empty page is encountered
                        total_path=None,
                        maximum_offset=27000,  # Optional: Limit the maximum offset to prevent infinite loops
                    ),
                )
            ),
        ],
    }

    yield from rest_api_resources(config)




# # execute nf_list_matkul_source and print the response for debugging
# pipeline = dlt.pipeline(
#     pipeline_name="quick_start",
#     destination="filesystem",
#     dataset_name="list_matkul",
#     # full_refresh=True # Optional: If you want to overwrite previous runs
# )
# load_info = pipeline.run(
#     nf_list_matkul_source(),
#     # table_name="list_matkul_tb",
#     loader_file_format="csv"
# )

# execute nf_list_matkul_source and print the response for debugging
# pipeline = dlt.pipeline(
#     pipeline_name="get_list_prodi",
#     destination="filesystem",
#     dataset_name="list_prodi",
#     # full_refresh=True # Optional: If you want to overwrite previous runs
# )
# load_info = pipeline.run(
#     nf_list_prodi_source(),
#     # table_name="list_matkul_tb",
#     loader_file_format="csv"
# )



# pipeline = dlt.pipeline(
#     pipeline_name="neofeeder_pipeline",
#     destination='duckdb',
#     dataset_name="prodi_list_data",
# )
# load_info = pipeline.run(nf_list_prodi_source())


pipeline = dlt.pipeline(
    pipeline_name="neofeeder_pipeline",
    destination='duckdb',
    dataset_name="matkul_list_data",
)
load_info = pipeline.run(nf_list_matkul_source())

print("===== Mantulity Response =====")
print(load_info)
