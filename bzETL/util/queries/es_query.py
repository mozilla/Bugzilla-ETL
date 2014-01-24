

def query(query):





"from": "test_data_all_dimensions",
"select": [
    {"value": "id", "name": "tdad_id"},
    "test_run_id",
    "revision",
    {"value": "n_replicates", "name": "count"},
    "mean",
    "std"
],
"edges": [
    "test_name",
    "product",
    "branch",
    "operating_system_name",
    "operating_system_version",
    "processor",
    "page_url"
],
"sort": {"name": "push_date", "value": "push_date"}

records_to_process = set(Q.select(db.query("""
        SELECT
            o.test_run_id
        FROM
            {{objectstore}}.objectstore o
        WHERE
            {{where}}
        ORDER BY
            o.test_run_id DESC
        LIMIT
            {{sample_limit}}
    """, {
    "objectstore": db.quote_column(settings.objectstore.schema),
    "sample_limit": SQL(settings.param.sustained_median.max_test_results_per_run),
    "where": db.esfilter2sqlwhere({"and": [
        {"term": {"o.processed_sustained_median": 'ready'}},
        {"term": {"o.processed_cube": "done"}}
    ]})
}), "test_run_id"))
